(ns study-rabbitmq.consistent-hash
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.exchange :as le]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]
            [clojure.tools.logging :as log])
  (:gen-class))

(def ^:const hash-exchange-name "study.hash.exchange")
(def ^:const hash-queue-prefix "study.hash.queue.")

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

(defn setup-consistent-hash-topology
  "Setup consistent hash exchange with multiple queues"
  [ch num-queues]
  (log/info "Setting up Consistent Hash Exchange topology...")
  
  ;; Create the consistent hash exchange
  ;; The 'x-consistent-hash' type distributes messages based on routing key hash
  (le/declare ch hash-exchange-name "x-consistent-hash" {:durable true})
  
  ;; Create multiple queues (like Kafka partitions)
  (doseq [i (range num-queues)]
    (let [queue-name (str hash-queue-prefix i)]
      (lq/declare ch queue-name {:durable true})
      ;; Bind queue with a weight (10 is a good default)
      ;; The weight determines how many hash buckets are assigned to this queue
      (lq/bind ch queue-name hash-exchange-name {:routing-key "10"})))
  
  (log/info (format "Consistent Hash topology setup complete with %d queues" num-queues)))

(defn message-handler
  "Handler for consuming messages from a specific queue"
  [queue-id ch {:keys [delivery-tag routing-key]} ^bytes payload]
  (let [message (String. payload "UTF-8")]
    (log/info (format "[Queue %d] Received message with routing-key '%s': %s" 
                      queue-id routing-key message))
    
    ;; Simulate processing
    (Thread/sleep (long (rand-int 500)))
    
    (log/info (format "[Queue %d] Processed: %s" queue-id message))
    (lb/ack ch delivery-tag)))

(defn start-consumer
  "Start a consumer for a specific queue"
  [conn queue-id]
  (let [ch (lch/open conn)
        queue-name (str hash-queue-prefix queue-id)]
    (log/info (format "Starting consumer for queue %d (%s)..." queue-id queue-name))
    (lc/subscribe ch queue-name (partial message-handler queue-id) {:auto-ack false})
    ch))

(defn start-producer
  "Start a producer that sends messages with user IDs as routing keys
   This ensures messages from the same user always go to the same queue (ordered processing)"
  [conn]
  (future
    (let [ch (lch/open conn)
          user-ids ["user-1" "user-2" "user-3" "user-4" "user-5"]]
      (log/info "Starting producer for Consistent Hash Exchange...")
      (loop [counter 0]
        (Thread/sleep (long (+ 1000 (rand-int 2000)))) ;; Random delay between 1-3 seconds

        ;; Pick a random user ID to demonstrate partition key behavior
        (let [user-id (rand-nth user-ids)
              message (format "Message %d for %s" counter user-id)]

          (log/info (format "[Producer] Publishing with routing-key '%s': %s" user-id message))
          ;; The routing key (user-id) acts like Kafka's partition key
          ;; Messages with the same routing key will always go to the same queue
          (lb/publish ch hash-exchange-name user-id message {:content-type "text/plain"
                                                             :persistent true})
          (recur (inc counter)))))))

(defn run-consistent-hash-example
  "Run the consistent hash exchange example"
  [num-queues]
  (log/info "=== Starting Consistent Hash Exchange Example ===")
  (log/info "This demonstrates ordered message processing similar to Kafka partition keys")
  (log/info (format "Messages with the same routing key (user-id) will always go to the same queue"))
  
  (Thread/sleep 5000) ;; Wait for RabbitMQ to be ready
  
  (try
    (let [conn (setup-connection)
          setup-ch (lch/open conn)]
      
      ;; Setup topology
      (setup-consistent-hash-topology setup-ch num-queues)
      (lch/close setup-ch)
      
      ;; Start consumers for each queue
      (log/info (format "Starting %d consumers (one per queue)..." num-queues))
      (let [consumers (doall (map #(start-consumer conn %) (range num-queues)))]
        
        ;; Start producer
        (log/info "Starting producer...")
        (let [producer (start-producer conn)]
          
          (log/info "Consistent Hash Exchange example running...")
          (log/info "Watch the logs to see how messages with the same user-id go to the same queue")
          (log/info "This ensures ordered processing per user, similar to Kafka partition keys")
          
          ;; Keep the application running
          @(promise))))
    
    (catch Exception e
      (log/error e "Error in Consistent Hash Exchange example")
      (System/exit 1))))

(defn -main
  [& args]
  ;; Run with 3 queues by default (like 3 Kafka partitions)
  (run-consistent-hash-example 3))
