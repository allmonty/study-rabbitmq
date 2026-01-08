(ns study-rabbitmq.core
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.exchange :as le]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]
            [clojure.tools.logging :as log]
            [study-rabbitmq.consistent-hash :as ch])
  (:gen-class))

(def ^:const exchange-name "study.exchange")
(def ^:const queue-name "study.queue")
(def ^:const dead-letter-exchange "study.dlx")
(def ^:const dead-letter-queue "study.dlq")
(def ^:const routing-key "study.routing.key")

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

(defn setup-topology
  "Setup exchanges, queues and bindings with dead letter configuration"
  [ch]
  (log/info "Setting up RabbitMQ topology...")
  
  ;; Create dead letter exchange and queue first
  (le/declare ch dead-letter-exchange "direct" {:durable true})
  (lq/declare ch dead-letter-queue {:durable true})
  (lq/bind ch dead-letter-queue dead-letter-exchange {:routing-key routing-key})
  
  ;; Create main exchange
  (le/declare ch exchange-name "direct" {:durable true})
  
  ;; Create main queue with dead letter configuration
  (lq/declare ch queue-name 
              {:durable true
               :arguments {"x-dead-letter-exchange" dead-letter-exchange
                          "x-dead-letter-routing-key" routing-key
                          "x-message-ttl" 30000}}) ;; 30 seconds TTL for testing
  
  (lq/bind ch queue-name exchange-name {:routing-key routing-key})
  (log/info "Topology setup complete"))

(defn message-handler
  "Handler for consuming messages"
  [consumer-id ch {:keys [delivery-tag]} ^bytes payload]
  (let [message (String. payload "UTF-8")]
    (log/info (format "[Consumer %d] Received: %s" consumer-id message))
    
    ;; Simulate processing and conditionally reject messages
    (Thread/sleep (long (rand-int 1000)))
    
    (if (.contains message "FAIL")
      (do
        (log/warn (format "[Consumer %d] Rejecting message (will go to DLQ): %s" consumer-id message))
        ;; Reject without requeue - message goes to dead letter queue
        (lb/reject ch delivery-tag false))
      (do
        (log/info (format "[Consumer %d] Acknowledging message: %s" consumer-id message))
        (lb/ack ch delivery-tag)))))

(defn start-consumer
  "Start a consumer that listens to the main queue"
  [conn consumer-id]
  (let [ch (lch/open conn)]
    (log/info (format "Starting consumer %d..." consumer-id))
    (lc/subscribe ch queue-name (partial message-handler consumer-id) {:auto-ack false})
    ch))

(defn start-producer
  "Start a producer that sends messages"
  [conn producer-id]
  (future
    (let [ch (lch/open conn)]
      (log/info (format "Starting producer %d..." producer-id))
      (loop [counter 0]
        (Thread/sleep (long (+ 2000 (rand-int 3000)))) ;; Random delay between 2-5 seconds

        ;; 20% chance to create a message that will be rejected
        (let [should-fail (< (rand) 0.2)
              message (if should-fail
                        (format "Producer %d - Message %d - FAIL" producer-id counter)
                        (format "Producer %d - Message %d - OK" producer-id counter))]

          (log/info (format "[Producer %d] Publishing: %s" producer-id message))
          (lb/publish ch exchange-name routing-key message {:content-type "text/plain"
                                                            :persistent true})
          (recur (inc counter)))))))

(defn -main
  [& args]
  (let [mode (or (System/getenv "RABBITMQ_MODE") "basic")]
    (log/info (format "Starting RabbitMQ Study Application in mode: %s" mode))
    
    (cond
      (= mode "consistent-hash")
      (do
        (log/info "Running Consistent Hash Exchange example...")
        (ch/run-consistent-hash-example 3))
      
      :else
      (do
        (log/info "Running basic exchange example with DLQ...")
        ;; Wait a bit for RabbitMQ to be ready
        (Thread/sleep 5000)
        
        (try
          (let [conn (setup-connection)
                setup-ch (lch/open conn)]
            
            ;; Setup topology
            (setup-topology setup-ch)
            (lch/close setup-ch)
            
            ;; Start 4 consumers
            (log/info "Starting 4 consumers...")
            (let [consumers (doall (map #(start-consumer conn %) (range 1 5)))]
              
              ;; Start 2 producers
              (log/info "Starting 2 producers...")
              (let [producers (doall (map #(start-producer conn %) (range 1 3)))]
                
                (log/info "Application running. Press Ctrl+C to stop.")
                (log/info "RabbitMQ Management UI available at http://localhost:15672")
                (log/info "Default credentials - username: guest, password: guest")
                
                ;; Keep the application running
                @(promise))))
          
          (catch Exception e
            (log/error e "Error in main application")
            (System/exit 1)))))))
