import { Observable } from 'rxjs';
import { StreamEvent, StreamSink } from '../../interfaces/Stream';
export interface ConsoleSinkConfig {
    prefix?: string;
    formatter?: (event: StreamEvent) => string;
}
export declare class ConsoleSink<T extends StreamEvent> implements StreamSink<T> {
    private readonly config;
    constructor(config?: ConsoleSinkConfig);
    write(data: Observable<T>): Promise<void>;
}
