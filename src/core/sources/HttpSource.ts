import { Observable, Subscriber } from 'rxjs';
import { StreamEvent, StreamSource } from '../../interfaces/Stream';
import * as http from 'http';
import { IncomingMessage, ServerResponse } from 'http';

export interface HttpSourceConfig {
  port: number;
  path: string;
  eventType: string;
}

export class HttpSource<T extends StreamEvent> implements StreamSource<T> {
  private server: http.Server | null = null;
  private events$: Observable<T>;
  private readonly config: HttpSourceConfig;

  constructor(config: HttpSourceConfig) {
    this.config = config;
    this.events$ = new Observable<T>((subscriber: Subscriber<T>) => {
      this.server = http.createServer((req: IncomingMessage, res: ServerResponse) => {
        if (req.url === this.config.path && req.method === 'POST') {
          let data = '';
          
          req.on('data', (chunk: string) => {
            data += chunk;
          });

          req.on('end', () => {
            try {
              const payload = JSON.parse(data);
              const event: T = {
                id: Math.random().toString(36).substring(7),
                timestamp: Date.now(),
                type: this.config.eventType,
                payload
              } as T;

              subscriber.next(event);
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ status: 'success' }));
            } catch (error) {
              res.writeHead(400, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ 
                status: 'error',
                message: 'Invalid JSON payload'
              }));
            }
          });
        } else {
          res.writeHead(404);
          res.end();
        }
      });

      this.server.listen(this.config.port);

      // Cleanup when unsubscribed
      return () => {
        if (this.server) {
          this.server.close();
          this.server = null;
        }
      };
    });
  }

  public read(): Observable<T> {
    return this.events$;
  }

  public stop(): void {
    if (this.server) {
      this.server.close();
      this.server = null;
    }
  }
} 