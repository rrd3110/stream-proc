import { Observable, map, filter } from 'rxjs';
import { StreamProcessor } from './StreamProcessor';
import { StreamEvent, StreamSource, StreamSink, WindowConfig } from '../interfaces/Stream';

/**
 * Fluent DSL for building stream processing pipelines
 */
export class StreamDSL<T extends StreamEvent = StreamEvent> {
  private processor!: StreamProcessor<T>;
  private currentPipeline: Observable<T> | null = null;

  constructor(source?: StreamSource<T>) {
    if (source) {
      this.processor = new StreamProcessor(source, {
        name: 'dsl-pipeline',
        parallelism: 1,
        bufferSize: 1000
      });
    }
  }

  /**
   * Sets the source for the pipeline
   */
  from(source: StreamSource<T>): StreamDSL<T> {
    this.processor = new StreamProcessor(source, {
      name: 'dsl-pipeline',
      parallelism: 1,
      bufferSize: 1000
    });
    return this;
  }

  /**
   * Applies a map transformation
   */
  map<R extends StreamEvent>(fn: (event: T) => R): StreamDSL<R> {
    this.processor.addTransformation({
      transform: (input: Observable<T>) => input.pipe(
        map(event => fn(event))
      )
    });
    return this as unknown as StreamDSL<R>;
  }

  /**
   * Applies a filter
   */
  where(predicate: (event: T) => boolean): StreamDSL<T> {
    this.processor.addFilter({
      filter: (input: Observable<T>) => input.pipe(
        filter(event => predicate(event))
      )
    });
    return this;
  }

  /**
   * Groups events by a key
   */
  keyBy<K>(keySelector: (event: T) => K): StreamDSL<T> {
    // Implementation would use groupBy operator
    return this;
  }

  /**
   * Applies windowing
   */
  window(type: WindowConfig['type'], size: number, slide?: number): StreamDSL<T> {
    const windowConfig: WindowConfig = {
      type,
      size,
      slide
    };
    // Implementation would use window operator
    return this;
  }

  /**
   * Counts events in windows
   */
  count(): StreamDSL<T> {
    // Implementation would use count operator
    return this;
  }

  /**
   * Calculates sum of a field in windows
   */
  sum(fieldSelector: (event: T) => number): StreamDSL<T> {
    // Implementation would use reduce operator
    return this;
  }

  /**
   * Calculates average of a field in windows
   */
  avg(fieldSelector: (event: T) => number): StreamDSL<T> {
    // Implementation would use reduce operator
    return this;
  }

  /**
   * Sets the sink for the pipeline
   */
  to(sink: StreamSink<T>): StreamDSL<T> {
    this.processor.addSink(sink);
    return this;
  }

  /**
   * Starts the pipeline
   */
  start(): void {
    this.processor.start();
  }

  /**
   * Stops the pipeline
   */
  stop(): void {
    this.processor.stop();
  }
} 