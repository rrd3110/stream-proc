import { StreamEvent, StreamSource, StreamSink, StreamTransformation, StreamFilter, StreamAggregator, WindowConfig, ProcessorConfig } from '../interfaces/Stream';
/**
 * Core stream processing engine
 */
export declare class StreamProcessor<T extends StreamEvent> {
    private source;
    private config;
    private logger;
    private subscriptions;
    private currentPipeline;
    private bloomFilter;
    constructor(source: StreamSource<T>, config: ProcessorConfig);
    /**
     * Sets up the logger with the configured log level
     */
    private setupLogger;
    /**
     * Initializes the bloom filter for deduplication if enabled
     */
    private initializeBloomFilter;
    /**
     * Adds a transformation to the pipeline
     */
    addTransformation<R extends StreamEvent>(transformation: StreamTransformation<T, R>): StreamProcessor<R>;
    /**
     * Adds a filter to the pipeline
     */
    addFilter(filter: StreamFilter<T>): StreamProcessor<T>;
    /**
     * Adds an aggregator with windowing to the pipeline
     */
    addAggregator<R extends StreamEvent>(aggregator: StreamAggregator<T, R>, windowConfig: WindowConfig): StreamProcessor<R>;
    /**
     * Applies a tumbling window to the stream
     */
    private applyTumblingWindow;
    /**
     * Applies a sliding window to the stream
     */
    private applySlidingWindow;
    /**
     * Applies a session window to the stream
     */
    private applySessionWindow;
    /**
     * Adds a sink to the pipeline and starts processing
     */
    addSink(sink: StreamSink<T>): void;
    /**
     * Handles errors based on the configured error policy
     */
    private handleError;
    /**
     * Starts the stream processing
     */
    start(): void;
    /**
     * Stops the stream processing and cleans up resources
     */
    stop(): void;
}
