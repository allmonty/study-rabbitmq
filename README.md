# Study RabbitMQ

A prototype Clojure application to study RabbitMQ communication patterns, including producers, consumers, dead letter queues, and consistent hash exchanges for ordered message processing.

## Overview

This project demonstrates:
- **2 Producers**: Create and publish messages to RabbitMQ
- **4 Consumers**: Read and acknowledge messages from the queue
- **Dead Letter Queue (DLQ)**: Messages marked as "FAIL" are rejected and routed to a dead letter queue
- **Consistent Hash Exchange**: Messages routed by hash of routing key, similar to Kafka partition keys for ordered processing
- **RabbitMQ Management UI**: Monitor messages and queues through a web browser

## Architecture

### Basic Exchange Example (Default)

- **Main Exchange**: `study.exchange` (direct)
- **Main Queue**: `study.queue` (with DLQ configuration)
- **Dead Letter Exchange**: `study.dlx` (direct)
- **Dead Letter Queue**: `study.dlq`

### Consistent Hash Exchange Example

- **Hash Exchange**: `study.hash.exchange` (x-consistent-hash)
- **Hash Queues**: `study.hash.queue.0`, `study.hash.queue.1`, `study.hash.queue.2` (3 queues like Kafka partitions)
- **Routing**: Messages with the same routing key (user-id) always go to the same queue, ensuring ordered processing

### Message Flow

1. Producers publish messages every 2-5 seconds
2. 20% of messages are marked with "FAIL" suffix
3. Consumers process messages:
   - Messages with "FAIL" are rejected → routed to DLQ
   - Normal messages are acknowledged
4. Messages also have a 30-second TTL for testing

## Prerequisites

- Docker and Docker Compose installed
- No other services running on ports 5672 (AMQP) and 15672 (Management UI)

## Running the Application

### Start the Basic Example (Default)

```bash
docker-compose up --build
```

This will:
1. Start RabbitMQ with management plugin and consistent hash exchange plugin
2. Build and start the Clojure application in basic mode
3. Initialize the queue topology
4. Start 2 producers and 4 consumers

### Start the Consistent Hash Exchange Example

To run the consistent hash exchange example (similar to Kafka partition keys):

```bash
docker-compose --profile consistent-hash up --build
```

This will:
1. Start RabbitMQ with the consistent hash exchange plugin enabled
2. Build and start the Clojure application in consistent-hash mode
3. Create a consistent hash exchange with 3 queues
4. Start 1 producer and 3 consumers (one per queue)
5. Demonstrate ordered message processing per user-id (routing key)

### Access RabbitMQ Management UI

Open your browser and navigate to:

```
http://localhost:15672
```

**Login credentials:**
- Username: `guest`
- Password: `guest`

### Monitoring in the UI

In the RabbitMQ Management UI, you can:

**For Basic Example:**
1. **Overview Tab**: See connection and message statistics
2. **Queues Tab**: 
   - View `study.queue` (main queue) - see messages being consumed
   - View `study.dlq` (dead letter queue) - see rejected messages
3. **Exchanges Tab**: View message routing through `study.exchange` and `study.dlx`

**For Consistent Hash Example:**
1. **Overview Tab**: See connection and message statistics
2. **Queues Tab**: 
   - View `study.hash.queue.0`, `study.hash.queue.1`, `study.hash.queue.2`
   - Notice messages are distributed across queues based on routing key hash
3. **Exchanges Tab**: View the `study.hash.exchange` (type: x-consistent-hash)

### Viewing Logs

To see application logs for the basic example:

```bash
docker-compose logs -f clojure-app
```

To see application logs for the consistent hash example:

```bash
docker-compose logs -f clojure-app-consistent-hash
```

**Basic example output:**
```
[Producer 1] Publishing: Producer 1 - Message 5 - OK
[Consumer 2] Received: Producer 1 - Message 5 - OK
[Consumer 2] Acknowledging message: Producer 1 - Message 5 - OK
[Producer 2] Publishing: Producer 2 - Message 3 - FAIL
[Consumer 1] Received: Producer 2 - Message 3 - FAIL
[Consumer 1] Rejecting message (will go to DLQ): Producer 2 - Message 3 - FAIL
```

**Consistent hash example output:**
```
[Producer] Publishing with routing-key 'user-1': Message 5 for user-1
[Queue 0] Received message with routing-key 'user-1': Message 5 for user-1
[Queue 0] Processed: Message 5 for user-1
[Producer] Publishing with routing-key 'user-1': Message 8 for user-1
[Queue 0] Received message with routing-key 'user-1': Message 8 for user-1
```

Notice how all messages with routing-key 'user-1' go to the same queue (Queue 0), ensuring ordered processing.

### Stop the Application

```bash
docker-compose down
```

## Project Structure

```
.
├── deps.edn                    # Clojure dependencies
├── docker-compose.yml          # Docker Compose configuration
├── Dockerfile                  # Clojure app container
├── src/
│   └── study_rabbitmq/
│       ├── core.clj           # Main application code (basic + DLQ example)
│       └── consistent_hash.clj # Consistent hash exchange example
└── README.md
```

## How It Works

### Basic Example - Producers and Consumers with DLQ

#### Producers
- Each producer runs in a separate thread
- Publishes messages every 2-5 seconds (random interval)
- 20% of messages are marked to fail (contain "FAIL" text)

#### Consumers
- 4 consumers subscribe to `study.queue`
- Messages are distributed round-robin among consumers
- Manual acknowledgment mode (auto-ack disabled)
- Messages with "FAIL" are rejected without requeue → go to DLQ
- Normal messages are acknowledged after processing

#### Dead Letter Queue
- Messages rejected by consumers go to `study.dlq`
- Messages that exceed 30-second TTL also go to DLQ
- Can be inspected in the Management UI

### Consistent Hash Exchange Example - Ordered Processing (Like Kafka Partition Keys)

#### What is Consistent Hash Exchange?

The Consistent Hash Exchange is a RabbitMQ plugin that routes messages to queues based on a hash of the routing key. This is similar to how Kafka uses partition keys to ensure messages with the same key go to the same partition, maintaining message ordering.

#### How it Works

1. **Exchange Type**: `x-consistent-hash` - distributes messages using consistent hashing
2. **Multiple Queues**: 3 queues are created (like 3 Kafka partitions)
3. **Routing Key as Partition Key**: The routing key (e.g., "user-1") determines which queue receives the message
4. **Consistent Routing**: All messages with the same routing key always go to the same queue
5. **Ordered Processing**: Since messages from the same user always go to the same queue and are processed by a single consumer, order is preserved

#### Example Flow

```
Producer sends:
  - Message 1 for user-1 (routing-key: "user-1") → Queue 0
  - Message 2 for user-2 (routing-key: "user-2") → Queue 1
  - Message 3 for user-1 (routing-key: "user-1") → Queue 0 (same as Message 1!)
  - Message 4 for user-3 (routing-key: "user-3") → Queue 2
  - Message 5 for user-1 (routing-key: "user-1") → Queue 0 (maintains order!)
```

#### Benefits

- **Ordered Processing**: Messages from the same user are processed in order
- **Scalability**: Different users can be processed in parallel across multiple queues/consumers
- **Kafka-like Behavior**: Similar to Kafka's partition key feature
- **Load Balancing**: Messages are distributed evenly across queues using consistent hashing

#### Use Cases

- Processing user events in order (e.g., user actions, state changes)
- Maintaining session consistency
- Sequential transaction processing per customer
- Any scenario where ordering matters within a logical grouping (user, session, tenant, etc.)

## Development

### Running Locally (without Docker)

1. Start RabbitMQ locally:
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.12-management
```

2. Run the Clojure application:
```bash
clojure -M:run
```

### Dependencies

- **Clojure 1.11.1**: Programming language
- **Langohr 5.4.0**: RabbitMQ client for Clojure
- **Logback**: Logging framework

## Learning Points

This prototype helps you understand:
- How to set up RabbitMQ exchanges and queues
- Producer-consumer patterns
- Message acknowledgment and rejection
- Dead letter queue configuration
- **Consistent Hash Exchange for ordered message processing**
- **How to achieve Kafka-like partition key behavior in RabbitMQ**
- **Message ordering guarantees using routing keys**
- Monitoring and debugging with RabbitMQ Management UI
- Containerization with Docker Compose