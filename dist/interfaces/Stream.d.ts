import { Observable } from 'rxjs';
/**
 * Base interface for stream events
 */
export interface StreamEvent {
    id: string;
    timestamp: number;
    type: string;
    payload: unknown;
}
/**
 * Interface for stream sources
 */
export interface StreamSource<T extends StreamEvent> {
    read(): Observable<T>;
    complete?(): void;
}
/**
 * Interface for stream sinks
 */
export interface StreamSink<T extends StreamEvent> {
    write(data: Observable<T>): Promise<void>;
    complete?(): void;
}
/**
 * Interface for stream transformations
 */
export interface StreamTransformation<T extends StreamEvent, R extends StreamEvent> {
    transform(input: Observable<T>): Observable<R>;
}
/**
 * Interface for stream filters
 */
export interface StreamFilter<T extends StreamEvent> {
    filter(input: Observable<T>): Observable<T>;
}
/**
 * Interface for stream aggregators
 */
export interface StreamAggregator<T extends StreamEvent, R extends StreamEvent> {
    aggregate(input: Observable<T>): Observable<R>;
}
/**
 * Interface for window configurations
 */
export interface WindowConfig {
    type: 'tumbling' | 'sliding' | 'session';
    size: number;
    slide?: number;
}
/**
 * Interface for processor configuration
 */
export interface ProcessorConfig {
    name: string;
    parallelism: number;
    bufferSize: number;
    maxRetries?: number;
    backoffPolicy?: 'fixed' | 'exponential';
    errorPolicy?: 'fail' | 'skip' | 'retry';
    checkpointInterval?: number;
    checkpointStorage?: 'memory' | 'file' | 'redis';
    checkpointConfig?: Record<string, unknown>;
    metrics?: boolean;
    metricsPort?: number;
    logLevel?: 'debug' | 'info' | 'warn' | 'error';
}
