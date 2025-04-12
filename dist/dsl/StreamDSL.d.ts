import { StreamEvent, WindowConfig } from '../interfaces/Stream';
import { StreamProcessor } from '../core/StreamProcessor';
export declare class StreamDSL<T extends StreamEvent> {
    private processor;
    constructor(processor: StreamProcessor<T>);
    map<R extends StreamEvent>(fn: (event: T) => R): StreamDSL<R>;
    where(predicate: (event: T) => boolean): StreamDSL<T>;
    window(config: WindowConfig): StreamDSL<T>;
    count(windowConfig: WindowConfig): StreamDSL<T>;
    sum(field: keyof T['payload'], windowConfig: WindowConfig): StreamDSL<T>;
    avg(field: keyof T['payload'], windowConfig: WindowConfig): StreamDSL<T>;
    private aggregate;
    start(): Promise<void>;
    stop(): void;
}
