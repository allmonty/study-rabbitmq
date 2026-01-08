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
       ;; Bind queue with a weight
        ;; The weight determines how many hash buckets are assigned to this queue
        ;; Documentation:
        ;; Binds a queue to a RabbitMQ consistent-hash exchange using the queue's weight
        ;; as the binding key (converted to a string).
        ;;
        ;; How weight = 10 makes the consistent-hash exchange work:
        ;; - In the consistent-hash exchange, a queue's binding key is treated as its
        ;;   weight (i.e., the number of virtual nodes/slots that queue occupies on the
        ;;   hash ring).
        ;; - Setting queue-weight to 10 and using (str queue-weight) causes the queue to
        ;;   be represented 10 times on the ring, increasing its share of the ring.
        ;; - When a message is published, the exchange hashes the message's routing key,
        ;;   maps the hash to a position on the ring, and delivers the message to the
        ;;   queue that owns that position.
        ;; - With weight 10, this queue will receive approximately 10× the messages of
        ;;   a queue with weight 1 (distribution is proportional to each queue's weight
        ;;   relative to the total weight of all bindings).
        ;;
        ;; Can this be any other number if all queues use the same weight?
        ;; - Yes. The exchange uses relative weights, so if every queue is bound with the
        ;;   same positive integer weight, the absolute value doesn't change distribution:
        ;;   using 1, 10 or 100 for all queues results in an equal split across queues.
        ;; - Use any positive integer; zero or negative values are invalid. Larger values
        ;;   increase the number of virtual nodes and give finer granularity, but provide
        ;;   no benefit when all queues have identical weights.
        ;; - If you want uneven distribution, give different queues different integer weights.
        ;;
        ;; Note: Binding keys are strings in AMQP, so converting the numeric weight to a
        ;; string is required.
      (lq/bind ch queue-name hash-exchange-name {:routing-key (str queue-weight)})))
  
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
        (Thread/sleep (long (+ min-publish-delay-ms (rand-int max-publish-delay-ms))))

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
