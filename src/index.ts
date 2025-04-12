// Core components
export { StreamProcessor } from './core/StreamProcessor';
export { StreamDSL } from './core/StreamDSL';

// Sources
export { HttpSource } from './sources/HttpSource';

// Sinks
export { ConsoleSink } from './sinks/ConsoleSink';

// Algorithms
export { WindowedBloomFilter } from './algorithms/WindowedBloomFilter';

// Interfaces
export {
  StreamEvent,
  StreamSource,
  StreamSink,
  StreamTransformation,
  StreamFilter,
  StreamAggregator,
  WindowConfig,
  ProcessorConfig
} from './interfaces/Stream'; 