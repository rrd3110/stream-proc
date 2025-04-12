import { Observable } from 'rxjs';
import { StreamEvent, StreamSink } from '../interfaces/Stream';
/**
 * Console sink that writes events to the console
 */
export declare class ConsoleSink<T extends StreamEvent> implements StreamSink<T> {
    private format;
    constructor(options?: {
        format?: 'json' | 'pretty';
    });
    /**
     * Writes events to the console
     */
    write(data: Observable<T>): Promise<void>;
    /**
     * Completes the sink
     */
    complete(): void;
}
