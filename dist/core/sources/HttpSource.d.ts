import { Observable } from 'rxjs';
import { StreamEvent, StreamSource } from '../../interfaces/Stream';
export interface HttpSourceConfig {
    port: number;
    path: string;
    eventType: string;
}
export declare class HttpSource<T extends StreamEvent> implements StreamSource<T> {
    private server;
    private events$;
    private readonly config;
    constructor(config: HttpSourceConfig);
    read(): Observable<T>;
    stop(): void;
}
