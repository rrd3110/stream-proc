import { Observable } from 'rxjs';
import { StreamEvent, StreamSource } from '../interfaces/Stream';
/**
 * HTTP source that listens for incoming requests
 */
export declare class HttpSource<T extends StreamEvent> implements StreamSource<T> {
    private server;
    private events$;
    private port;
    private path;
    private method;
    constructor(options: {
        port: number;
        path?: string;
        method?: string;
    });
    /**
     * Starts the HTTP server
     */
    start(): Promise<void>;
    /**
     * Stops the HTTP server
     */
    stop(): Promise<void>;
    /**
     * Handles incoming HTTP requests
     */
    private handleRequest;
    /**
     * Returns an observable of events
     */
    read(): Observable<T>;
    /**
     * Completes the event stream
     */
    complete(): void;
}
