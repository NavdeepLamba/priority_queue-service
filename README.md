# Priority Queue Service

A Java-based priority queue service with both in-memory and cloud-based implementations. This project enhances a basic message queue with priority functionality, where messages can have numerical priority values and are delivered in priority order with FIFO ordering within the same priority level.

## Features

- **Priority-based message delivery**: Messages with higher numerical priority values are delivered first
- **FIFO ordering within same priority**: Messages with the same priority are delivered in first-in-first-out order
- **Multiple implementations**: 
  - In-memory priority queue using Java's `PriorityBlockingQueue`
  - Cloud-based implementation using Upstash Redis HTTP API
- **Thread-safe operations**: All implementations support concurrent access
- **Visibility timeouts**: Messages become invisible during processing to prevent duplicate processing
- **Negative priority support**: Handles negative priority values correctly

## Architecture

The system follows a common interface design pattern:

```
QueueService (Interface)
├── InMemoryPriorityQueueService (In-memory implementation)
└── UpstashQueueService (Cloud Redis implementation)
```

### Core Components

- **`QueueService`**: Interface defining core operations (`push`, `pull`, `delete`)
- **`Message`**: Message entity with priority, timestamp, and visibility timeout support
- **`InMemoryPriorityQueueService`**: Thread-safe in-memory priority queue
- **`UpstashQueueService`**: Distributed cloud queue using Upstash Redis

## Requirements

- Java 17 or higher
- Maven 3.6 or higher
- For Upstash implementation: Valid Upstash Redis credentials

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   mvn clean install
   ```

## Usage

### In-Memory Priority Queue

```java
InMemoryPriorityQueueService queue = new InMemoryPriorityQueueService();

// Push messages with different priorities
queue.push("my-queue", "Low priority task", 1);
queue.push("my-queue", "High priority alert!", 10);
queue.push("my-queue", "Normal task", 5);

// Pull messages (they come out in priority order)
Message msg = queue.pull("my-queue");
if (msg != null) {
    System.out.println("Received: " + msg.getBody() + " (priority: " + msg.getPriority() + ")");
    // Process the message...
    queue.delete("my-queue", msg.getReceiptId());
}
```

### Upstash Redis Priority Queue

First, set up your Upstash Redis credentials:

```bash
export UPSTASH_REDIS_REST_URL="https://your-database.upstash.io"
export UPSTASH_REDIS_REST_TOKEN="your-token-here"
```

Then use the queue:

```java
UpstashQueueService queue = new UpstashQueueService();

// Same API as in-memory implementation
queue.push("distributed-queue", "Critical alert", 15);
queue.push("distributed-queue", "Background job", 3);

Message msg = queue.pull("distributed-queue");
if (msg != null) {
    // Process message...
    queue.delete("distributed-queue", msg.getReceiptId());
}
```

## Priority Queue Behavior

### Priority Ordering
- Higher numerical values = higher priority
- Messages are delivered in descending priority order
- Example: priority 10 > priority 5 > priority 1 > priority 0 > priority -1

### FIFO Within Same Priority
- Messages with identical priority are delivered in timestamp order
- Earlier messages are delivered first within the same priority level

### Example Priority Flow
```
Push Order:    A(pri:1) → B(pri:10) → C(pri:5) → D(pri:5)
Pull Order:    B(pri:10) → C(pri:5) → D(pri:5) → A(pri:1)
                          ↑ FIFO within same priority ↑
```

## Configuration

Configure the service via `src/main/resources/config.properties`:

```properties
# Visibility timeout in seconds
visibilityTimeout=30

# Default priority for messages when not specified
defaultPriority=0
```

## Testing

Run the test suite:

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=InMemoryPriorityQueueServiceTest

# Run specific test method
mvn test -Dtest=InMemoryPriorityQueueServiceTest#testPriorityOrdering
```

### Test Coverage

The test suite includes:
- Basic push/pull operations
- Priority ordering verification
- FIFO behavior within same priority
- Negative priority handling
- Concurrent operations
- Visibility timeout functionality
- Multiple queue independence

## Implementation Details

### In-Memory Implementation
- Uses Java's `PriorityBlockingQueue` with custom comparator
- Thread-safe using concurrent collections
- Messages stored in memory with priority and timestamp
- Invisible messages tracked separately with TTL

### Upstash Implementation
- Uses Redis sorted sets for priority ordering
- Composite scoring: `(priority * 1B) + (MAX_LONG - timestamp)`
- HTTP REST API calls for all operations
- Atomic operations using Redis commands
- Message content stored in Redis hashes
- Invisible messages with TTL for automatic cleanup

