import { StreamEvent, StreamSource, StreamSink, WindowConfig } from '../interfaces/Stream';
/**
 * Fluent DSL for building stream processing pipelines
 */
export declare class StreamDSL<T extends StreamEvent = StreamEvent> {
    private processor;
    private currentPipeline;
    constructor(source?: StreamSource<T>);
    /**
     * Sets the source for the pipeline
     */
    from(source: StreamSource<T>): StreamDSL<T>;
    /**
     * Applies a map transformation
     */
    map<R extends StreamEvent>(fn: (event: T) => R): StreamDSL<R>;
    /**
     * Applies a filter
     */
    where(predicate: (event: T) => boolean): StreamDSL<T>;
    /**
     * Groups events by a key
     */
    keyBy<K>(keySelector: (event: T) => K): StreamDSL<T>;
    /**
     * Applies windowing
     */
    window(type: WindowConfig['type'], size: number, slide?: number): StreamDSL<T>;
    /**
     * Counts events in windows
     */
    count(): StreamDSL<T>;
    /**
     * Calculates sum of a field in windows
     */
    sum(fieldSelector: (event: T) => number): StreamDSL<T>;
    /**
     * Calculates average of a field in windows
     */
    avg(fieldSelector: (event: T) => number): StreamDSL<T>;
    /**
     * Sets the sink for the pipeline
     */
    to(sink: StreamSink<T>): StreamDSL<T>;
    /**
     * Starts the pipeline
     */
    start(): void;
    /**
     * Stops the pipeline
     */
    stop(): void;
}
