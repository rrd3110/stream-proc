import { Observable, Subject, Subscription } from 'rxjs';
import { StreamEvent, StreamSource, StreamSink, StreamTransformation, StreamFilter, StreamAggregator, WindowConfig, ProcessorConfig } from '../interfaces/Stream';
import { BloomFilter } from 'bloomfilter';
import winston from 'winston';

/**
 * Core stream processing engine
 */
export class StreamProcessor<T extends StreamEvent> {
  private source: StreamSource<T>;
  private config: ProcessorConfig;
  private logger: winston.Logger;
  private subscriptions: Subscription[] = [];
  private currentPipeline: Observable<T> | null = null;
  private bloomFilter: BloomFilter | null = null;

  constructor(source: StreamSource<T>, config: ProcessorConfig) {
    this.source = source;
    this.config = config;
    this.logger = this.setupLogger();
    this.initializeBloomFilter();
  }

  /**
   * Sets up the logger with the configured log level
   */
  private setupLogger(): winston.Logger {
    return winston.createLogger({
      level: this.config.logLevel || 'info',
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
      ),
      transports: [
        new winston.transports.Console()
      ]
    });
  }

  /**
   * Initializes the bloom filter for deduplication if enabled
   */
  private initializeBloomFilter(): void {
    if (this.config.metrics) {
      this.bloomFilter = new BloomFilter(
        32 * 256, // number of bits to allocate
        16        // number of hash functions
      );
    }
  }

  /**
   * Adds a transformation to the pipeline
   */
  addTransformation<R extends StreamEvent>(transformation: StreamTransformation<T, R>): StreamProcessor<R> {
    if (!this.currentPipeline) {
      this.currentPipeline = this.source.read();
    }
    const transformedPipeline = transformation.transform(this.currentPipeline);
    this.currentPipeline = transformedPipeline as unknown as Observable<T>;
    return this as unknown as StreamProcessor<R>;
  }

  /**
   * Adds a filter to the pipeline
   */
  addFilter(filter: StreamFilter<T>): StreamProcessor<T> {
    if (!this.currentPipeline) {
      this.currentPipeline = this.source.read();
    }
    this.currentPipeline = filter.filter(this.currentPipeline);
    return this;
  }

  /**
   * Adds an aggregator with windowing to the pipeline
   */
  addAggregator<R extends StreamEvent>(
    aggregator: StreamAggregator<T, R>,
    windowConfig: WindowConfig
  ): StreamProcessor<R> {
    if (!this.currentPipeline) {
      this.currentPipeline = this.source.read();
    }

    // Apply windowing based on configuration
    let windowedStream: Observable<T>;
    switch (windowConfig.type) {
      case 'tumbling':
        windowedStream = this.applyTumblingWindow(this.currentPipeline, windowConfig.size);
        break;
      case 'sliding':
        windowedStream = this.applySlidingWindow(
          this.currentPipeline,
          windowConfig.size,
          windowConfig.slide || windowConfig.size
        );
        break;
      case 'session':
        windowedStream = this.applySessionWindow(this.currentPipeline, windowConfig.size);
        break;
      default:
        throw new Error(`Unsupported window type: ${windowConfig.type}`);
    }

    const aggregatedPipeline = aggregator.aggregate(windowedStream);
    this.currentPipeline = aggregatedPipeline as unknown as Observable<T>;
    return this as unknown as StreamProcessor<R>;
  }

  /**
   * Applies a tumbling window to the stream
   */
  private applyTumblingWindow(stream: Observable<T>, windowSize: number): Observable<T> {
    return stream.pipe(
      // Implementation using RxJS window operator
      // This is a placeholder - actual implementation would use window operator
      // and buffer the events within each window
    );
  }

  /**
   * Applies a sliding window to the stream
   */
  private applySlidingWindow(stream: Observable<T>, windowSize: number, slideSize: number): Observable<T> {
    return stream.pipe(
      // Implementation using RxJS window operator with slide
      // This is a placeholder - actual implementation would use window operator
      // with slide parameter and buffer the events within each window
    );
  }

  /**
   * Applies a session window to the stream
   */
  private applySessionWindow(stream: Observable<T>, inactivityGap: number): Observable<T> {
    return stream.pipe(
      // Implementation using RxJS window operator with session window
      // This is a placeholder - actual implementation would use window operator
      // with session window logic based on inactivity gap
    );
  }

  /**
   * Adds a sink to the pipeline and starts processing
   */
  addSink(sink: StreamSink<T>): void {
    if (!this.currentPipeline) {
      this.currentPipeline = this.source.read();
    }

    const subscription = this.currentPipeline.subscribe({
      next: (event) => {
        this.logger.debug('Processing event', { eventId: event.id, type: event.type });
        if (this.bloomFilter && !this.bloomFilter.test(event.id)) {
          this.bloomFilter.add(event.id);
          sink.write(new Observable(subscriber => subscriber.next(event)));
        }
      },
      error: (error) => {
        this.logger.error('Error in pipeline', { error });
        this.handleError(error);
      },
      complete: () => {
        this.logger.info('Pipeline completed');
        sink.complete?.();
      }
    });

    this.subscriptions.push(subscription);
  }

  /**
   * Handles errors based on the configured error policy
   */
  private handleError(error: Error): void {
    switch (this.config.errorPolicy) {
      case 'fail':
        this.logger.error('Failing pipeline due to error', { error });
        this.stop();
        throw error;
      case 'skip':
        this.logger.warn('Skipping error', { error });
        break;
      case 'retry':
        this.logger.info('Retrying operation', { error });
        // Implement retry logic with backoff
        break;
      default:
        this.logger.error('Unhandled error', { error });
        this.stop();
        throw error;
    }
  }

  /**
   * Starts the stream processing
   */
  start(): void {
    this.logger.info('Starting stream processor', { config: this.config });
    if (!this.currentPipeline) {
      this.currentPipeline = this.source.read();
    }
  }

  /**
   * Stops the stream processing and cleans up resources
   */
  stop(): void {
    this.logger.info('Stopping stream processor');
    this.subscriptions.forEach(sub => sub.unsubscribe());
    this.source.complete?.();
    this.currentPipeline = null;
  }
} 