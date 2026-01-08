# Study RabbitMQ

A prototype Clojure application to study RabbitMQ communication patterns, including producers, consumers, and dead letter queues.

## Overview

This project demonstrates:
- **2 Producers**: Create and publish messages to RabbitMQ
- **4 Consumers**: Read and acknowledge messages from the queue
- **Dead Letter Queue (DLQ)**: Messages marked as "FAIL" are rejected and routed to a dead letter queue
- **RabbitMQ Management UI**: Monitor messages and queues through a web browser

## Architecture

- **Main Exchange**: `study.exchange` (direct)
- **Main Queue**: `study.queue` (with DLQ configuration)
- **Dead Letter Exchange**: `study.dlx` (direct)
- **Dead Letter Queue**: `study.dlq`

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

### Start Everything

```bash
docker-compose up --build
```

This will:
1. Start RabbitMQ with management plugin
2. Build and start the Clojure application
3. Initialize the queue topology
4. Start 2 producers and 4 consumers

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

1. **Overview Tab**: See connection and message statistics
2. **Queues Tab**: 
   - View `study.queue` (main queue) - see messages being consumed
   - View `study.dlq` (dead letter queue) - see rejected messages
3. **Exchanges Tab**: View message routing through `study.exchange` and `study.dlx`

### Viewing Logs

To see application logs:

```bash
docker-compose logs -f clojure-app
```

You'll see output like:
```
[Producer 1] Publishing: Producer 1 - Message 5 - OK
[Consumer 2] Received: Producer 1 - Message 5 - OK
[Consumer 2] Acknowledging message: Producer 1 - Message 5 - OK
[Producer 2] Publishing: Producer 2 - Message 3 - FAIL
[Consumer 1] Received: Producer 2 - Message 3 - FAIL
[Consumer 1] Rejecting message (will go to DLQ): Producer 2 - Message 3 - FAIL
```

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
│       └── core.clj           # Main application code
└── README.md
```

## How It Works

### Producers
- Each producer runs in a separate thread
- Publishes messages every 2-5 seconds (random interval)
- 20% of messages are marked to fail (contain "FAIL" text)

### Consumers
- 4 consumers subscribe to `study.queue`
- Messages are distributed round-robin among consumers
- Manual acknowledgment mode (auto-ack disabled)
- Messages with "FAIL" are rejected without requeue → go to DLQ
- Normal messages are acknowledged after processing

### Dead Letter Queue
- Messages rejected by consumers go to `study.dlq`
- Messages that exceed 30-second TTL also go to DLQ
- Can be inspected in the Management UI

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
- Monitoring and debugging with RabbitMQ Management UI
- Containerization with Docker Compose