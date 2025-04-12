"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.StreamProcessor = void 0;
const rxjs_1 = require("rxjs");
const bloomfilter_1 = require("bloomfilter");
const winston_1 = __importDefault(require("winston"));
/**
 * Core stream processing engine
 */
class StreamProcessor {
    constructor(source, config) {
        this.subscriptions = [];
        this.currentPipeline = null;
        this.bloomFilter = null;
        this.source = source;
        this.config = config;
        this.logger = this.setupLogger();
        this.initializeBloomFilter();
    }
    /**
     * Sets up the logger with the configured log level
     */
    setupLogger() {
        return winston_1.default.createLogger({
            level: this.config.logLevel || 'info',
            format: winston_1.default.format.combine(winston_1.default.format.timestamp(), winston_1.default.format.json()),
            transports: [
                new winston_1.default.transports.Console()
            ]
        });
    }
    /**
     * Initializes the bloom filter for deduplication if enabled
     */
    initializeBloomFilter() {
        if (this.config.metrics) {
            this.bloomFilter = new bloomfilter_1.BloomFilter(32 * 256, // number of bits to allocate
            16 // number of hash functions
            );
        }
    }
    /**
     * Adds a transformation to the pipeline
     */
    addTransformation(transformation) {
        if (!this.currentPipeline) {
            this.currentPipeline = this.source.read();
        }
        const transformedPipeline = transformation.transform(this.currentPipeline);
        this.currentPipeline = transformedPipeline;
        return this;
    }
    /**
     * Adds a filter to the pipeline
     */
    addFilter(filter) {
        if (!this.currentPipeline) {
            this.currentPipeline = this.source.read();
        }
        this.currentPipeline = filter.filter(this.currentPipeline);
        return this;
    }
    /**
     * Adds an aggregator with windowing to the pipeline
     */
    addAggregator(aggregator, windowConfig) {
        if (!this.currentPipeline) {
            this.currentPipeline = this.source.read();
        }
        // Apply windowing based on configuration
        let windowedStream;
        switch (windowConfig.type) {
            case 'tumbling':
                windowedStream = this.applyTumblingWindow(this.currentPipeline, windowConfig.size);
                break;
            case 'sliding':
                windowedStream = this.applySlidingWindow(this.currentPipeline, windowConfig.size, windowConfig.slide || windowConfig.size);
                break;
            case 'session':
                windowedStream = this.applySessionWindow(this.currentPipeline, windowConfig.size);
                break;
            default:
                throw new Error(`Unsupported window type: ${windowConfig.type}`);
        }
        const aggregatedPipeline = aggregator.aggregate(windowedStream);
        this.currentPipeline = aggregatedPipeline;
        return this;
    }
    /**
     * Applies a tumbling window to the stream
     */
    applyTumblingWindow(stream, windowSize) {
        return stream.pipe(
        // Implementation using RxJS window operator
        // This is a placeholder - actual implementation would use window operator
        // and buffer the events within each window
        );
    }
    /**
     * Applies a sliding window to the stream
     */
    applySlidingWindow(stream, windowSize, slideSize) {
        return stream.pipe(
        // Implementation using RxJS window operator with slide
        // This is a placeholder - actual implementation would use window operator
        // with slide parameter and buffer the events within each window
        );
    }
    /**
     * Applies a session window to the stream
     */
    applySessionWindow(stream, inactivityGap) {
        return stream.pipe(
        // Implementation using RxJS window operator with session window
        // This is a placeholder - actual implementation would use window operator
        // with session window logic based on inactivity gap
        );
    }
    /**
     * Adds a sink to the pipeline and starts processing
     */
    addSink(sink) {
        if (!this.currentPipeline) {
            this.currentPipeline = this.source.read();
        }
        const subscription = this.currentPipeline.subscribe({
            next: (event) => {
                this.logger.debug('Processing event', { eventId: event.id, type: event.type });
                if (this.bloomFilter && !this.bloomFilter.test(event.id)) {
                    this.bloomFilter.add(event.id);
                    sink.write(new rxjs_1.Observable(subscriber => subscriber.next(event)));
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
    handleError(error) {
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
    start() {
        this.logger.info('Starting stream processor', { config: this.config });
        if (!this.currentPipeline) {
            this.currentPipeline = this.source.read();
        }
    }
    /**
     * Stops the stream processing and cleans up resources
     */
    stop() {
        this.logger.info('Stopping stream processor');
        this.subscriptions.forEach(sub => sub.unsubscribe());
        this.source.complete?.();
        this.currentPipeline = null;
    }
}
exports.StreamProcessor = StreamProcessor;
//# sourceMappingURL=StreamProcessor.js.map