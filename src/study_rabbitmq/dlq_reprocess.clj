(ns study-rabbitmq.dlq-reprocess
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.exchange :as le]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]
            [clojure.tools.logging :as log])
  (:gen-class))

(def ^:const hash-exchange-name "study.dlq-reprocess.hash.exchange")
(def ^:const hash-queue-prefix "study.dlq-reprocess.hash.queue.")
(def ^:const dead-letter-exchange "study.dlq-reprocess.dlx")
(def ^:const dead-letter-queue "study.dlq-reprocess.dlq")
(def ^:const queue-weight 10) ;; Weight determines hash bucket distribution
(def ^:const min-publish-delay-ms 1000)
(def ^:const max-publish-delay-ms 2000)

(defn setup-connection
  "Create connection to RabbitMQ"
  []
  (let [host (or (System/getenv "RABBITMQ_HOST") "localhost")
        port (Integer/parseInt (or (System/getenv "RABBITMQ_PORT") "5672"))
        user (or (System/getenv "RABBITMQ_USER") "guest")
        password (or (System/getenv "RABBITMQ_PASSWORD") "guest")]
    (log/info (format "Connecting to RabbitMQ at %s:%d" host port))
    (rmq/connect {:host host
                  :port port
                  :username user
                  :password password})))

(defn setup-dlq-reprocess-topology
  "Setup consistent hash exchange with dead letter queue configuration"
  [ch num-queues]
  (log/info "Setting up DLQ Reprocess with Consistent Hash Exchange topology...")
  
  ;; Create dead letter exchange and queue first
  (le/declare ch dead-letter-exchange "direct" {:durable true})
  (lq/declare ch dead-letter-queue {:durable true})
  (lq/bind ch dead-letter-queue dead-letter-exchange {:routing-key "dlq"})
  
  ;; Create the consistent hash exchange
  ;; The 'x-consistent-hash' type distributes messages based on routing key hash
  (le/declare ch hash-exchange-name "x-consistent-hash" {:durable true})
  
  ;; Create multiple queues (like Kafka partitions) with DLQ configuration
  (doseq [i (range num-queues)]
    (let [queue-name (str hash-queue-prefix i)]
      (lq/declare ch queue-name 
                  {:durable true
                   :arguments {"x-dead-letter-exchange" dead-letter-exchange
                              "x-dead-letter-routing-key" "dlq"
                              "x-message-ttl" 30000}}) ;; 30 seconds TTL for testing
      ;; Bind queue with a weight
      (lq/bind ch queue-name hash-exchange-name {:routing-key (str queue-weight)})))
  
  (log/info (format "DLQ Reprocess topology setup complete with %d queues and DLQ configured" num-queues)))

(defn message-handler
  "Handler for consuming messages from a specific queue"
  [queue-id ch {:keys [delivery-tag routing-key]} ^bytes payload]
  (let [message (String. payload "UTF-8")]
    (log/info (format "[Queue %d] Received message with routing-key '%s': %s" 
                      queue-id routing-key message))
    
    ;; Simulate processing
    (Thread/sleep (long (rand-int 500)))
    
    ;; Check if message should fail (contains "FAIL")
    (if (.contains message "FAIL")
      (do
        (log/warn (format "[Queue %d] Rejecting message (will go to DLQ): %s" queue-id message))
        ;; Reject without requeue - message goes to dead letter queue
        (lb/reject ch delivery-tag false))
      (do
        (log/info (format "[Queue %d] Processed and acknowledged: %s" queue-id message))
        (lb/ack ch delivery-tag)))))

(defn dlq-consumer-handler
  "Special handler for consuming messages from the dead letter queue
   This consumer logs the message and republishes it back to the main exchange"
  [conn ch {:keys [delivery-tag]} ^bytes payload]
  (let [message (String. payload "UTF-8")
        republish-ch (lch/open conn)]
    (log/info (format "[DLQ Consumer] Received dead letter message: %s" message))
    (log/info (format "[DLQ Consumer] Reprocessing and republishing message to main exchange..."))
    
    ;; Extract the original user-id from the message (assuming format: "Message X for user-Y")
    ;; For reprocessing, we'll use the same routing key to maintain consistent hashing
    (let [user-id-match (re-find #"(user-\d+)" message)
          user-id (if user-id-match
                    (second user-id-match)
                    "user-1")] ;; fallback to user-1 if pattern doesn't match
      
      (log/info (format "[DLQ Consumer] Republishing with routing-key '%s': %s" user-id message))
      
      ;; Republish the message to the main exchange with the original routing key
      (lb/publish republish-ch hash-exchange-name user-id message 
                  {:content-type "text/plain"
                   :persistent true})
      
      ;; Acknowledge the message from DLQ after successful republish
      (lb/ack ch delivery-tag)
      (lch/close republish-ch)
      
      (log/info (format "[DLQ Consumer] Successfully reprocessed and acknowledged: %s" message)))))

(defn start-consumer
  "Start a consumer for a specific queue"
  [conn queue-id]
  (let [ch (lch/open conn)
        queue-name (str hash-queue-prefix queue-id)]
    (log/info (format "Starting consumer for queue %d (%s)..." queue-id queue-name))
    (lc/subscribe ch queue-name (partial message-handler queue-id) {:auto-ack false})
    ch))

(defn start-dlq-consumer
  "Start a special consumer for the dead letter queue that republishes messages"
  [conn]
  (let [ch (lch/open conn)]
    (log/info (format "Starting DLQ consumer for reprocessing (%s)..." dead-letter-queue))
    (lc/subscribe ch dead-letter-queue (partial dlq-consumer-handler conn) {:auto-ack false})
    ch))

(defn start-producer
  "Start a producer that sends messages with user IDs as routing keys
   This ensures messages from the same user always go to the same queue (ordered processing)"
  [conn]
  (future
    (let [ch (lch/open conn)
          user-ids ["user-1" "user-2" "user-3" "user-4" "user-5"]]
      (log/info "Starting producer for DLQ Reprocess with Consistent Hash Exchange...")
      (loop [counter 0]
        (Thread/sleep (long (+ min-publish-delay-ms (rand-int max-publish-delay-ms))))

        ;; Pick a random user ID to demonstrate partition key behavior
        (let [user-id (rand-nth user-ids)
              ;; 30% chance to create a message that will be rejected and go to DLQ
              should-fail (< (rand) 0.3)
              message (if should-fail
                        (format "Message %d for %s - FAIL" counter user-id)
                        (format "Message %d for %s" counter user-id))]

          (log/info (format "[Producer] Publishing with routing-key '%s': %s" user-id message))
          ;; The routing key (user-id) acts like Kafka's partition key
          ;; Messages with the same routing key will always go to the same queue
          (lb/publish ch hash-exchange-name user-id message {:content-type "text/plain"
                                                             :persistent true})
          (recur (inc counter)))))))

(defn run-dlq-reprocess-example
  "Run the DLQ reprocess with consistent hash exchange example"
  [num-queues]
  (log/info "=== Starting DLQ Reprocess with Consistent Hash Exchange Example ===")
  (log/info "This demonstrates:")
  (log/info "1. Ordered message processing per user-id (like Kafka partition keys)")
  (log/info "2. Failed messages go to Dead Letter Queue")
  (log/info "3. DLQ consumer automatically reprocesses and republishes dead letters")
  (log/info (format "Messages with the same routing key (user-id) will always go to the same queue"))
  
  (Thread/sleep 5000) ;; Wait for RabbitMQ to be ready
  
  (try
    (let [conn (setup-connection)
          setup-ch (lch/open conn)]
      
      ;; Setup topology
      (setup-dlq-reprocess-topology setup-ch num-queues)
      (lch/close setup-ch)
      
      ;; Start consumers for each queue
      (log/info (format "Starting %d consumers (one per queue)..." num-queues))
      (let [consumers (doall (map #(start-consumer conn %) (range num-queues)))]
        
        ;; Start DLQ consumer for reprocessing
        (log/info "Starting DLQ consumer for automatic reprocessing...")
        (let [dlq-consumer (start-dlq-consumer conn)]
          
          ;; Start producer
          (log/info "Starting producer...")
          (let [producer (start-producer conn)]
            
            (log/info "DLQ Reprocess example running...")
            (log/info "Watch the logs to see:")
            (log/info "  - How messages with the same user-id go to the same queue (consistent hashing)")
            (log/info "  - How FAIL messages are rejected and go to DLQ")
            (log/info "  - How DLQ consumer automatically reprocesses and republishes dead letters")
            
            ;; Keep the application running
            @(promise)))))
    
    (catch Exception e
      (log/error e "Error in DLQ Reprocess example")
      (System/exit 1))))

(defn -main
  [& args]
  ;; Run with 3 queues by default (like 3 Kafka partitions)
  (run-dlq-reprocess-example 3))
