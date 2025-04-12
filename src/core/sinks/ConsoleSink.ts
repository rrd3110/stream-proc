import { Observable, Subscriber } from 'rxjs';
import { StreamEvent, StreamSink } from '../../interfaces/Stream';

export interface ConsoleSinkConfig {
  prefix?: string;
  formatter?: (event: StreamEvent) => string;
}

export class ConsoleSink<T extends StreamEvent> implements StreamSink<T> {
  private readonly config: ConsoleSinkConfig;

  constructor(config: ConsoleSinkConfig = {}) {
    this.config = {
      prefix: '📊 ',
      formatter: (event: StreamEvent) => 
        JSON.stringify(event, null, 2),
      ...config
    };
  }

  public async write(data: Observable<T>): Promise<void> {
    return new Promise((resolve, reject) => {
      const subscription = data.subscribe({
        next: (event: T) => {
          const output = this.config.formatter!(event);
          console.log(`${this.config.prefix}${output}`);
        },
        error: (error: Error) => {
          console.error(`${this.config.prefix}Error:`, error);
          reject(error);
        },
        complete: () => {
          console.log(`${this.config.prefix}Stream completed`);
          resolve();
        }
      });

      // Handle cleanup
      return () => subscription.unsubscribe();
    });
  }
} 