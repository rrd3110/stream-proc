import { Observable } from 'rxjs';
import { StreamEvent, StreamSink } from '../interfaces/Stream';

/**
 * Console sink that writes events to the console
 */
export class ConsoleSink<T extends StreamEvent> implements StreamSink<T> {
  private format: 'json' | 'pretty';

  constructor(options: { format?: 'json' | 'pretty' } = {}) {
    this.format = options.format || 'pretty';
  }

  /**
   * Writes events to the console
   */
  write(data: Observable<T>): Promise<void> {
    return new Promise((resolve, reject) => {
      data.subscribe({
        next: (event) => {
          if (this.format === 'json') {
            console.log(JSON.stringify(event));
          } else {
            console.log(`[${new Date(event.timestamp).toISOString()}] ${event.type}:`, event.payload);
          }
        },
        error: (error) => {
          console.error('Error in console sink:', error);
          reject(error);
        },
        complete: () => {
          resolve();
        }
      });
    });
  }

  /**
   * Completes the sink
   */
  complete(): void {
    console.log('Console sink completed');
  }
} 