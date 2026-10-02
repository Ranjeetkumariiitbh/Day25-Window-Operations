# Day25 — Window Operations

A practical Apache Spark Streaming (DStream) project covering:

- Batch interval
- Window size
- Sliding interval
- `countByWindow`
- `reduceByKeyAndWindow`
- Rolling sales totals
- Sudden transaction-increase detection

## Project goal

Build practical confidence with Spark Streaming window operations.

### Scenario

Transactions arrive continuously as:

```text
timestamp,customerId,category,amount
```

The application keeps a rolling window of transactions and:

1. Counts transactions with `countByWindow`.
2. Calculates rolling sales totals by category with `reduceByKeyAndWindow`.
3. Calculates rolling transaction counts per customer.
4. Raises an alert when the number of transactions in the active window reaches the configured threshold.

## Window concepts

Suppose:

```text
Batch interval   = 2 seconds
Window size      = 10 minutes
Sliding interval = 2 minutes
```

- **Batch interval**: how frequently Spark creates a micro-batch.
- **Window size**: how much recent data is included in a window.
- **Sliding interval**: how frequently the window moves and produces a new result.

The project uses a **2-second batch interval**, a **10-minute production window**, and a **2-minute production slide**.

For a fast local demonstration, run with the `demo` argument. The demo uses a 10-second window and a 4-second slide so results appear quickly.

## Requirements

- Java 17
- Scala 2.12
- sbt 1.10+
- Apache Spark 3.5.1
- Netcat (`nc`)

## Project structure

```text
Day25-Window-Operations/
├── build.sbt
├── project/
│   └── build.properties
├── src/
│   └── main/
│       └── scala/
│           └── com/
│               └── day25/
│                   └── Day25WindowOperations.scala
└── README.md
```

## Run

### 1. Start the input stream

Terminal 1:

```bash
nc -lk 9999
```

If your `nc` version uses `-l -p`, use:

```bash
nc -l -p 9999
```

### 2. Run the fast demo

Terminal 2:

```bash
sbt "run demo"
```

Choose:

```text
batch interval   = 2 seconds
window size      = 10 seconds
slide interval   = 4 seconds
```

### 3. Send sample transactions

Paste lines into the `nc` terminal:

```text
2026-10-02T10:00:00,C01,electronics,1200
2026-10-02T10:00:01,C02,grocery,450
2026-10-02T10:00:02,C01,electronics,800
2026-10-02T10:00:03,C03,fashion,1500
2026-10-02T10:00:04,C02,grocery,300
2026-10-02T10:00:05,C04,electronics,2000
```

You should see rolling output similar to:

```text
[countByWindow] ... last_10s_transactions=6 | ALERT: sudden increase in transactions

[reduceByKeyAndWindow] ...
  electronics    -> ₹4000.00
  fashion        -> ₹1500.00
  grocery        -> ₹750.00
```

Exact output depends on when the records arrive relative to the micro-batch boundaries.

## Run the production-style 10-minute window

```bash
sbt run
```

This uses:

```text
Batch interval   = 2 seconds
Window size      = 600 seconds (10 minutes)
Sliding interval = 120 seconds (2 minutes)
```

## Transformation vs action

### Transformations

- `flatMap`
- `map`
- `reduceByKeyAndWindow`
- `countByWindow`

### Actions / output operations

- `foreachRDD`
- `collect`

`foreachRDD` is where the rolling results are consumed/printed.

## Partition and shuffle considerations

`reduceByKeyAndWindow` is a key-based aggregation. It can require data movement between partitions when keys are not already co-located.

Important points:

- `reduceByKeyAndWindow` performs a distributed key aggregation.
- More partitions can improve parallelism when the data volume is large.
- Too many partitions create scheduling and task overhead.
- `collect()` is used here only because this is a small learning/demo project.
- In production, avoid collecting large windows to the driver. Write results to a database, Kafka, dashboard sink, or distributed storage.
- Window operations retain/process multiple batches, so memory and checkpointing strategy matter for real workloads.

## Performance notes

### 1. Batch interval

A smaller batch interval can reduce processing latency but creates more scheduling overhead.

### 2. Window size

A larger window contains more historical data and can increase state and computation requirements.

### 3. Sliding interval

A smaller slide produces results more frequently, increasing output and processing overhead.

### 4. `reduceByKeyAndWindow`

For large workloads, prefer incremental/reduced window strategies where appropriate and use sensible partitioning.

### 5. `collect()`

This project uses `collect()` only for demonstration. Never use it to bring an unbounded/large production result to the driver.

## What this project demonstrates

```text
Continuous transactions
        |
        v
+---------------------+
| Spark DStream       |
| batch interval 2s   |
+----------+----------+
           |
           v
+---------------------+
| Sliding Window      |
| 10 min / 2 min      |
+----------+----------+
           |
      +----+----+
      |         |
      v         v
countByWindow  reduceByKeyAndWindow
      |         |
      v         v
transaction     rolling sales
count           by category
      |
      v
sudden increase alert
```

## Key takeaway

Window operations allow Spark Streaming to answer questions over recent data rather than over the complete stream:

> "How many transactions arrived recently?"

> "What are the sales totals in the recent window?"

> "Has transaction volume suddenly increased?"

That is the core of this Day 25 exercise.
