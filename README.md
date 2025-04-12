# Stream Processor

[![npm version](https://badge.fury.io/js/stream-processor.svg)](https://badge.fury.io/js/stream-processor)
[![Build Status](https://travis-ci.org/yourusername/stream-processor.svg?branch=main)](https://travis-ci.org/yourusername/stream-processor)
[![Coverage Status](https://coveralls.io/repos/github/yourusername/stream-processor/badge.svg?branch=main)](https://coveralls.io/github/yourusername/stream-processor?branch=main)
[![Downloads](https://img.shields.io/npm/dm/stream-processor.svg)](https://www.npmjs.com/package/stream-processor)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance data stream processor with features like event sourcing, sliding window aggregations, and concurrency methods for real-time processing of high-throughput event streams.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
  - [Using the DSL](#using-the-dsl)
  - [Using the Core API](#using-the-core-api)
- [API Reference](#api-reference)
  - [Sources](#sources)
  - [Transformations](#transformations)
  - [Filters](#filters)
  - [Windows and Aggregations](#windows-and-aggregations)
  - [Sinks](#sinks)
  - [Configuration Options](#configuration-options)
- [Advanced Use Cases](#advanced-use-cases)
- [Performance Tuning](#performance-tuning)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Event Sourcing**: Capture and process events in real-time from multiple sources
- **Windowed Aggregations**: Support for tumbling, sliding, and session windows
- **Concurrency**: Parallel processing with configurable parallelism levels
- **DSL Interface**: Fluent API for building stream processing pipelines
- **Extensible**: Custom sources, sinks, transformations, and aggregations
- **High Performance**: Optimized for throughput using modern concurrency patterns
- **Back-pressure Handling**: Built-in mechanisms to handle varying data rates
- **Fault Tolerance**: Recovery strategies for handling failures gracefully
- **Metrics & Monitoring**: Built-in instrumentation for observability

## Installation

```bash
npm install stream-processor
```

Or using yarn:

```bash
yarn add stream-processor
```

## Quick Start

```typescript
import { StreamDSL } from 'stream-processor';
import { HttpSource } from 'stream-processor/sources';
import { ConsoleSink } from 'stream-processor/sinks';

// Create a simple pipeline that processes HTTP events
const pipeline = new StreamDSL()
  .from(new HttpSource({ port: 3000 }))
  .map(event => ({ ...event, timestamp: Date.now() }))
  .window('tumbling', 1000)
  .count()
  .to(new ConsoleSink());

// Start processing
pipeline.start();
```

## Usage Examples

### Using the DSL

The Domain Specific Language (DSL) approach provides a fluent, chainable API for building processing pipelines:

```typescript
import { StreamDSL } from 'stream-processor';
import { KafkaSource } from 'stream-processor/sources';
import { ElasticsearchSink } from 'stream-processor/sinks';

const pipeline = new StreamDSL()
  // Consume from Kafka topic
  .from(new KafkaSource({ 
    brokers: ['localhost:9092'], 
    topic: 'user-events',
    groupId: 'processor-group'
  }))
  
  // Parse JSON events
  .map(event => JSON.parse(event.value))
  
  // Add processing timestamp
  .map(event => ({ 
    ...event, 
    processedAt: Date.now() 
  }))
  
  // Filter out test events
  .where(event => !event.isTest)
  
  // Group by user ID
  .keyBy(event => event.userId)
  
  // Count events per user in 5-minute sliding windows
  .window('sliding', {
    size: 5 * 60 * 1000,    // 5 minutes
    slide: 60 * 1000        // Slide every 1 minute
  })
  .count()
  
  // Alert on high activity
  .where(counts => counts.value > 100)
  
  // Send to Elasticsearch
  .to(new ElasticsearchSink({
    node: 'http://localhost:9200',
    index: 'user-activity-alerts'
  }));

pipeline.start();
```

### Using the Core API

For more complex scenarios or finer control, the Core API provides direct access to the underlying components:

```typescript
import { StreamProcessor } from 'stream-processor';
import { KafkaSource } from 'stream-processor/sources';
import { ElasticsearchSink } from 'stream-processor/sinks';
import { map, filter } from 'stream-processor/operators';

// Create a processor with a Kafka source
const processor = new StreamProcessor(
  new KafkaSource({
    brokers: ['localhost:9092'],
    topic: 'user-events',
    groupId: 'processor-group'
  }), 
  {
    name: 'user-activity-monitor',
    parallelism: 4,
    bufferSize: 10000,
    checkpointInterval: 1000,
    errorPolicy: 'retry'
  }
);

// Build the processing pipeline
const pipeline = processor
  // Parse and enrich events
  .addTransformation({
    name: 'parse-and-enrich',
    transform: input => input.pipe(
      map(event => {
        const parsed = JSON.parse(event.value);
        return {
          ...parsed,
          processedAt: Date.now(),
          region: getRegionFromIp(parsed.ipAddress)
        };
      })
    )
  })
  
  // Filter out test events
  .addFilter({
    name: 'production-only',
    filter: input => input.pipe(
      filter(event => !event.isTest && event.environment === 'production')
    )
  })
  
  // Add custom business logic
  .addTransformation({
    name: 'risk-scoring',
    transform: input => input.pipe(
      map(event => ({
        ...event,
        riskScore: calculateRiskScore(event)
      }))
    )
  })
  
  // Use a bloom filter to detect duplicate events
  .addFilter({
    name: 'deduplication',
    filter: new WindowedBloomFilter({
      size: 10000,
      errorRate: 0.001,
      windowSize: 60 * 60 * 1000  // 1 hour
    })
  })
  
  // Aggregate by user ID
  .addAggregator(
    {
      name: 'user-activity-count',
      keyBy: event => event.userId,
      aggregate: input => input.pipe(count())
    },
    {
      type: 'sliding',
      size: 5 * 60 * 1000,  // 5 minutes
      slide: 60 * 1000      // Slide every 1 minute
    }
  );

// Add a sink for high-activity users
pipeline.addSink(
  new ElasticsearchSink({
    node: 'http://localhost:9200',
    index: 'user-activity-metrics',
    batchSize: 100
  }),
  input => input.pipe(filter(metric => metric.value > 100))
);

// Add monitoring sink for all metrics
pipeline.addSink(
  new PrometheusSink({
    port: 9090,
    path: '/metrics',
    metricName: 'user_activity_count'
  })
);

// Start processing
pipeline.start();
```

## API Reference

### Sources

Stream Processor includes several built-in sources:

| Source | Description | Options |
|--------|-------------|---------|
| `HttpSource` | Listens for HTTP requests | `{ port, path, method }` |
| `KafkaSource` | Consumes from Kafka topics | `{ brokers, topic, groupId, ... }` |
| `FileSource` | Reads from file(s) | `{ path, watch, encoding }` |
| `S3Source` | Reads from AWS S3 | `{ bucket, prefix, credentials }` |
| `WebSocketSource` | Listens for WebSocket connections | `{ port, path }` |
| `SQLSource` | Polls a database table | `{ connection, query, interval }` |

Creating a custom source:

```typescript
import { Source } from 'stream-processor';

class MyCustomSource extends Source {
  constructor(options) {
    super(options);
  }
  
  initialize() {
    // Setup your source
  }
  
  start() {
    // Start emitting events
  }
  
  stop() {
    // Clean up resources
  }
}
```

### Transformations

Transformations modify each event in the stream:

| Transformation | Description |
|----------------|-------------|
| `map` | Changes each event using a mapping function |
| `flatMap` | Maps and flattens the results |
| `deduplicate` | Removes duplicate events |
| `enrich` | Adds data to events from external sources |

Example:
```typescript
// Using the DSL
pipeline
  .map(event => ({ ...event, processed: true }))
  .flatMap(event => event.items.map(item => ({ ...item, parentId: event.id })))
  .deduplicate(event => event.id)
  .enrich(async event => {
    const userData = await userService.getUser(event.userId);
    return { ...event, userData };
  });

// Using the Core API
processor.addTransformation({
  name: 'enrich-events',
  transform: input => input.pipe(
    map(event => ({ ...event, processed: true })),
    flatMap(event => event.items.map(item => ({ ...item, parentId: event.id }))),
    deduplicate(event => event.id),
    enrich(async event => {
      const userData = await userService.getUser(event.userId);
      return { ...event, userData };
    })
  )
});
```

### Filters

Filters determine which events continue in the pipeline:

| Filter | Description |
|--------|-------------|
| `where` | Keeps events that match a predicate |
| `sample` | Samples events at a specified rate |
| `throttle` | Limits the rate of events |
| `WindowedBloomFilter` | Efficient approximate deduplication |

Example:
```typescript
// Using the DSL
pipeline
  .where(event => event.value > 10)
  .sample(0.1)  // Sample 10% of events
  .throttle(1000);  // Max 1000 events per second

// Using the Core API
processor
  .addFilter({
    name: 'high-value-events',
    filter: input => input.pipe(filter(event => event.value > 10))
  })
  .addFilter({
    name: 'sampling',
    filter: input => input.pipe(sample(0.1))
  })
  .addFilter({
    name: 'rate-limiting',
    filter: input => input.pipe(throttle(1000))
  });
```

### Windows and Aggregations

Windows group events for aggregation:

| Window Type | Description |
|-------------|-------------|
| `tumbling` | Fixed-size, non-overlapping windows |
| `sliding` | Fixed-size, overlapping windows |
| `session` | Dynamic windows based on activity |
| `global` | Single window for all events |

Aggregation functions:

| Aggregation | Description |
|-------------|-------------|
| `count` | Counts events |
| `sum` | Sums a specified field |
| `avg` | Calculates average of a field |
| `min` | Finds minimum value |
| `max` | Finds maximum value |
| `topK` | Gets top K elements |
| `distinct` | Counts distinct values |
| `percentile` | Calculates percentiles (approximate) |

Example:
```typescript
// Using the DSL
pipeline
  .keyBy(event => event.userId)
  .window('sliding', {
    size: 5 * 60 * 1000,    // 5 minutes
    slide: 60 * 1000        // Slide every 1 minute
  })
  .aggregate([
    count().as('eventCount'),
    sum(e => e.value).as('totalValue'),
    avg(e => e.duration).as('avgDuration'),
    percentile(e => e.responseTime, 95).as('p95ResponseTime')
  ]);

// Using the Core API
processor.addAggregator(
  {
    name: 'user-metrics',
    keyBy: event => event.userId,
    aggregate: input => input.pipe(
      aggregate([
        count().as('eventCount'),
        sum(e => e.value).as('totalValue'),
        avg(e => e.duration).as('avgDuration'),
        percentile(e => e.responseTime, 95).as('p95ResponseTime')
      ])
    )
  },
  {
    type: 'sliding',
    size: 5 * 60 * 1000,
    slide: 60 * 1000
  }
);
```

### Sinks

Sinks output processed data:

| Sink | Description | Options |
|------|-------------|---------|
| `ConsoleSink` | Outputs to console | `{ format }` |
| `FileSink` | Writes to file | `{ path, format, append }` |
| `KafkaSink` | Produces to Kafka | `{ brokers, topic, ... }` |
| `ElasticsearchSink` | Writes to Elasticsearch | `{ node, index, ... }` |
| `HttpSink` | Makes HTTP requests | `{ url, method, headers }` |
| `S3Sink` | Writes to AWS S3 | `{ bucket, prefix, credentials }` |
| `PrometheusSink` | Exposes metrics | `{ port, path, metricName }` |

Creating a custom sink:

```typescript
import { Sink } from 'stream-processor';

class MyCustomSink extends Sink {
  constructor(options) {
    super(options);
  }
  
  initialize() {
    // Setup your sink
  }
  
  write(event) {
    // Write the event
  }
  
  close() {
    // Clean up resources
  }
}
```

### Configuration Options

The StreamProcessor constructor accepts these configuration options:

```typescript
{
  // Basic settings
  name: string;              // Name of the processor
  parallelism: number;       // Number of parallel workers (default: # of CPU cores)
  bufferSize: number;        // Event buffer size (default: 1000)
  
  // Advanced settings
  maxRetries: number;        // Maximum retries for failed events (default: 3)
  backoffPolicy: 'fixed' | 'exponential'; // Retry backoff strategy (default: exponential)
  errorPolicy: 'fail' | 'skip' | 'retry'; // How to handle errors (default: retry)
  
  // Checkpointing & recovery
  checkpointInterval: number; // Milliseconds between checkpoints (default: 60000)
  checkpointStorage: 'memory' | 'file' | 'redis'; // Where to store checkpoints
  checkpointConfig: object;   // Configuration for checkpoint storage
  
  // Monitoring & metrics
  metrics: boolean;           // Enable metrics (default: true)
  metricsPort: number;        // Port for metrics server (default: 9090)
  logLevel: 'debug' | 'info' | 'warn' | 'error'; // Logging level (default: info)
}
```

## Advanced Use Cases

### Fraud Detection System

```typescript
const fraudDetectionPipeline = new StreamDSL()
  .from(new KafkaSource({ 
    brokers: ['kafka:9092'], 
    topic: 'transactions'
  }))
  .map(event => JSON.parse(event.value))
  .keyBy(tx => tx.userId)
  
  // Calculate velocity: how many transactions in last 5 minutes
  .window('sliding', { size: 5 * 60 * 1000, slide: 10 * 1000 })
  .count().as('txVelocity')
  
  // Detect anomalous behavior
  .where(metric => metric.value > 10) // More than 10 transactions in 5 minutes
  
  // Enrich with historical data
  .map(async alert => {
    const userHistory = await getUserTransactionHistory(alert.key);
    return {
      ...alert,
      historicalAverage: userHistory.avgTxPerHour,
      riskScore: calculateRiskScore(alert.value, userHistory)
    };
  })
  
  // High risk alerts go to security team
  .where(alert => alert.riskScore > 0.8)
  .to(new AlertSink({ 
    endpoint: 'https://security-api/fraud-alerts',
    headers: { 'Authorization': 'Bearer token' }
  }));
```

### IoT Sensor Monitoring

```typescript
const iotMonitoringPipeline = new StreamDSL()
  .from(new MQTTSource({ 
    brokerUrl: 'mqtt://broker.hivemq.com', 
    topic: 'sensors/#' 
  }))
  .map(event => JSON.parse(event.payload))
  
  // Add timestamp and device metadata
  .enrich(async reading => {
    const deviceInfo = await deviceRegistry.getDevice(reading.deviceId);
    return {
      ...reading,
      timestamp: Date.now(),
      deviceType: deviceInfo.type,
      location: deviceInfo.location
    };
  })
  
  // Group by device type and location
  .keyBy(reading => `${reading.deviceType}:${reading.location}`)
  
  // Monitor for anomalies in 1-minute windows
  .window('tumbling', 60 * 1000)
  .aggregate([
    avg(r => r.temperature).as('avgTemp'),
    min(r => r.temperature).as('minTemp'),
    max(r => r.temperature).as('maxTemp'),
    stdDev(r => r.temperature).as('tempStdDev')
  ])
  
  // Detect anomalies
  .where(stats => {
    // If standard deviation is high or temps are outside normal range
    return stats.tempStdDev > 10 || 
           stats.maxTemp > 100 || 
           stats.minTemp < 0;
  })
  
  // Send to time-series database
  .to(new InfluxDBSink({
    url: 'http://influxdb:8086',
    database: 'sensor_metrics',
    measurement: 'temperature_anomalies'
  }))
  
  // Also trigger alerts
  .to(new WebhookSink({
    url: 'https://monitoring-service/alerts',
    method: 'POST'
  }));
```

### Log Processing and Analytics

```typescript
const logAnalyticsPipeline = new StreamDSL()
  .from(new FileSource({ 
    path: '/var/log/application*.log', 
    watch: true 
  }))
  
  // Parse log lines
  .map(line => parseLogLine(line))
  
  // Filter out healthy responses
  .where(log => log.level === 'ERROR' || log.responseTime > 500)
  
  // Group by API endpoint
  .keyBy(log => log.endpoint)
  
  // Analyze in 5-minute windows
  .window('tumbling', 5 * 60 * 1000)
  .aggregate([
    count().as('errorCount'),
    avg(l => l.responseTime).as('avgResponseTime'),
    percentile(l => l.responseTime, 95).as('p95ResponseTime'),
    percentile(l => l.responseTime, 99).as('p99ResponseTime'),
    countDistinct(l => l.userId).as('affectedUsers')
  ])
  
  // Store metrics
  .to(new ElasticsearchSink({
    node: 'http://elasticsearch:9200',
    index: 'api_metrics',
    idField: 'endpoint'
  }))
  
  // Expose metrics for Grafana
  .to(new PrometheusSink({
    port: 9090,
    metrics: {
      errorCount: 'counter',
      avgResponseTime: 'gauge',
      p95ResponseTime: 'gauge',
      p99ResponseTime: 'gauge',
      affectedUsers: 'gauge'
    }
  }));
```

## Performance Tuning

### Memory Management

Control memory usage by adjusting buffer sizes and backpressure:

```typescript
const pipeline = new StreamProcessor(mySource, {
  bufferSize: 5000,                  // Increase buffer for bursty workloads
  maxPendingEvents: 10000,           // Set max pending events before backpressure
  backpressureStrategy: 'drop',      // 'drop' or 'block'
  gcThreshold: 0.8                   // Trigger GC when heap usage reaches 80%
});
```

### Parallelism and Partitioning

Optimize for multi-core systems:

```typescript
const pipeline = new StreamProcessor(mySource, {
  parallelism: 8,                    // Process on 8 parallel workers
  partitioningStrategy: 'consistent-hash', // How to distribute work
  partitioningKey: event => event.userId, // Key for partitioning
  workerAffinityEnabled: true        // Pin workers to CPU cores
});
```

### Batching and Windowing

Configure batch processing for higher throughput:

```typescript
const pipeline = new StreamDSL()
  .from(mySource)
  .batch({
    size: 1000,                      // Process up to 1000 events at once
    timeout: 100                     // Or every 100ms, whichever comes first
  })
  .map(batch => processBatch(batch))
  .window('tumbling', {
    size: 60000,                     // 1-minute windows
    allowedLateness: 10000,          // Handle late events up to 10s
    watermarkStrategy: 'system-time' // Or 'event-time' with extractTimestamp
  })
  .aggregate(myAggregation);
```

## Troubleshooting

### Common Issues and Solutions

| Issue | Possible Causes | Solutions |
|-------|----------------|-----------|
| Memory leaks | Unclosed resources, reference cycles | Use `pipeline.cleanup()`, check for event listeners |
| High latency | Buffer overflow, GC pauses | Adjust buffer sizes, tune JVM settings |
| Data loss | Unreliable sources, error handling | Enable checkpointing, implement retry logic |
| Pipeline stalls | Blocking operations | Use async operations, increase parallelism |
| Slow sinks | Network issues, back pressure | Implement batching, monitor sink performance |

### Debugging Tools

The library provides tools to help diagnose issues:

```typescript
// Enable debug logging
const pipeline = new StreamDSL({ logLevel: 'debug' });

// Use the built-in metrics to identify bottlenecks
pipeline.metrics.enable({
  console: true,           // Print metrics to console
  prometheus: 9090,        // Expose Prometheus metrics
  statsd: {                // Send to StatsD
    host: 'localhost',
    port: 8125,
    prefix: 'stream-processor'
  }
});

// Add checkpoint for recovery in case of failures
pipeline.checkpoint({
  interval: 30000,         // Checkpoint every 30 seconds
  storage: 'redis',        // Use Redis for checkpoint storage
  config: {
    host: 'localhost',
    port: 6379
  }
});

// Debug specific components
pipeline.debug('transformation1');   // Debug a specific transformation
```

### Error Handling Strategies

```typescript
// Global error handler
const pipeline = new StreamDSL({
  errorPolicy: 'retry',
  maxRetries: 3,
  backoffPolicy: 'exponential',
  deadLetterQueue: new KafkaSink({
    topic: 'error-events',
    brokers: ['localhost:9092'] 
  })
});

// Component-specific error handling
pipeline
  .from(mySource)
  .map(event => {
    try {
      return transform(event);
    } catch (error) {
      pipeline.logger.warn('Transform error', { error, event });
      return { ...event, error: error.message, status: 'failed' };
    }
  })
  .to(mySink);
```

## Contributing

We welcome contributions to the Stream Processor project!

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone the repository
git clone https://github.com/rrd3110/stream-proc.git
cd stream-proc

# Install dependencies
npm install

# Run tests
npm test

# Build the project
npm run build

# Run the linter
npm run lint
```

### Code Style

We use ESLint and Prettier to maintain code quality. Please run:

```bash
npm run lint
npm run format
```

before submitting your PR.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
