import { Observable, Subject } from 'rxjs';
import { StreamEvent, StreamSource } from '../interfaces/Stream';
import * as http from 'http';
import { v4 as uuidv4 } from 'uuid';

/**
 * HTTP source that listens for incoming requests
 */
export class HttpSource<T extends StreamEvent> implements StreamSource<T> {
  private server: http.Server;
  private events$ = new Subject<T>();
  private port: number;
  private path: string;
  private method: string;

  constructor(options: {
    port: number;
    path?: string;
    method?: string;
  }) {
    this.port = options.port;
    this.path = options.path || '/events';
    this.method = options.method || 'POST';
    this.server = http.createServer(this.handleRequest.bind(this));
  }

  /**
   * Starts the HTTP server
   */
  start(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server.listen(this.port, () => {
        console.log(`HTTP source listening on port ${this.port}`);
        resolve();
      });
      this.server.on('error', reject);
    });
  }

  /**
   * Stops the HTTP server
   */
  stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server.close((err) => {
        if (err) {
          reject(err);
        } else {
          this.events$.complete();
          resolve();
        }
      });
    });
  }

  /**
   * Handles incoming HTTP requests
   */
  private handleRequest(req: http.IncomingMessage, res: http.ServerResponse): void {
    if (req.url === this.path && req.method === this.method) {
      let body = '';
      req.on('data', chunk => {
        body += chunk.toString();
      });
      req.on('end', () => {
        try {
          const payload = JSON.parse(body);
          const event: T = {
            id: uuidv4(),
            timestamp: Date.now(),
            type: 'http',
            payload
          } as T;
          this.events$.next(event);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ status: 'success' }));
        } catch (error) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Invalid JSON' }));
        }
      });
    } else {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Not Found' }));
    }
  }

  /**
   * Returns an observable of events
   */
  read(): Observable<T> {
    return this.events$.asObservable();
  }

  /**
   * Completes the event stream
   */
  complete(): void {
    this.events$.complete();
  }
} 