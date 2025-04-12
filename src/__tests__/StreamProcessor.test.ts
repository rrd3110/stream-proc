import { Observable, Subject } from 'rxjs';
import { StreamProcessor } from '../core/StreamProcessor';
import { StreamEvent, StreamSource } from '../interfaces/Stream';

// Mock event type for testing
interface TestEvent extends StreamEvent {
  payload: {
    value: number;
  };
}

// Mock source that emits test events
class TestSource implements StreamSource<TestEvent> {
  private events$ = new Subject<TestEvent>();

  read(): Observable<TestEvent> {
    return this.events$.asObservable();
  }

  // Helper method to emit test events
  emit(event: TestEvent): void {
    this.events$.next(event);
  }

  complete(): void {
    this.events$.complete();
  }
}

describe('StreamProcessor', () => {
  let source: TestSource;
  let processor: StreamProcessor<TestEvent>;

  beforeEach(() => {
    source = new TestSource();
    processor = new StreamProcessor(source, {
      name: 'test-processor',
      parallelism: 1,
      bufferSize: 10
    });
  });

  test('should process events through map transformation', (done) => {
    const pipeline = processor
      .addTransformation({
        transform: (input: Observable<TestEvent>) => 
          input.pipe(map(event => ({
            ...event,
            payload: { value: event.payload.value * 2 }
          })))
      });

    const results: TestEvent[] = [];

    pipeline.addSink({
      write: (data: Observable<TestEvent>) => {
        return new Promise((resolve) => {
          data.subscribe({
            next: (event) => results.push(event),
            complete: () => {
              expect(results).toHaveLength(1);
              expect(results[0].payload.value).toBe(20);
              resolve();
              done();
            }
          });
        });
      }
    });

    pipeline.start();

    source.emit({
      id: '1',
      timestamp: Date.now(),
      type: 'test',
      payload: { value: 10 }
    });

    source.complete();
  });

  test('should filter events correctly', (done) => {
    const pipeline = processor
      .addFilter({
        filter: (input: Observable<TestEvent>) => 
          input.pipe(filter(event => event.payload.value > 10))
      });

    const results: TestEvent[] = [];

    pipeline.addSink({
      write: (data: Observable<TestEvent>) => {
        return new Promise((resolve) => {
          data.subscribe({
            next: (event) => results.push(event),
            complete: () => {
              expect(results).toHaveLength(1);
              expect(results[0].payload.value).toBe(20);
              resolve();
              done();
            }
          });
        });
      }
    });

    pipeline.start();

    source.emit({
      id: '1',
      timestamp: Date.now(),
      type: 'test',
      payload: { value: 5 }
    });

    source.emit({
      id: '2',
      timestamp: Date.now(),
      type: 'test',
      payload: { value: 20 }
    });

    source.complete();
  });

  test('should aggregate events in windows', (done) => {
    const pipeline = processor
      .addAggregator(
        {
          aggregate: (input: Observable<TestEvent>) => {
            let sum = 0;
            let count = 0;
            return new Observable<StreamEvent>(subscriber => {
              input.subscribe({
                next: (event) => {
                  sum += event.payload.value;
                  count++;
                  subscriber.next({
                    id: '1',
                    timestamp: Date.now(),
                    type: 'aggregated',
                    payload: {
                      average: sum / count
                    }
                  });
                },
                error: (err) => subscriber.error(err),
                complete: () => subscriber.complete()
              });
            });
          }
        },
        {
          type: 'tumbling',
          size: 100
        }
      );

    const results: StreamEvent[] = [];

    pipeline.addSink({
      write: (data: Observable<StreamEvent>) => {
        return new Promise((resolve) => {
          data.subscribe({
            next: (event) => results.push(event),
            complete: () => {
              expect(results).toHaveLength(2);
              expect(results[1].payload.average).toBe(15);
              resolve();
              done();
            }
          });
        });
      }
    });

    pipeline.start();

    source.emit({
      id: '1',
      timestamp: Date.now(),
      type: 'test',
      payload: { value: 10 }
    });

    source.emit({
      id: '2',
      timestamp: Date.now(),
      type: 'test',
      payload: { value: 20 }
    });

    source.complete();
  });
}); 