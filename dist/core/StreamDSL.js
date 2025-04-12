"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.StreamDSL = void 0;
const rxjs_1 = require("rxjs");
const StreamProcessor_1 = require("./StreamProcessor");
/**
 * Fluent DSL for building stream processing pipelines
 */
class StreamDSL {
    constructor(source) {
        this.currentPipeline = null;
        if (source) {
            this.processor = new StreamProcessor_1.StreamProcessor(source, {
                name: 'dsl-pipeline',
                parallelism: 1,
                bufferSize: 1000
            });
        }
    }
    /**
     * Sets the source for the pipeline
     */
    from(source) {
        this.processor = new StreamProcessor_1.StreamProcessor(source, {
            name: 'dsl-pipeline',
            parallelism: 1,
            bufferSize: 1000
        });
        return this;
    }
    /**
     * Applies a map transformation
     */
    map(fn) {
        this.processor.addTransformation({
            transform: (input) => input.pipe((0, rxjs_1.map)(event => fn(event)))
        });
        return this;
    }
    /**
     * Applies a filter
     */
    where(predicate) {
        this.processor.addFilter({
            filter: (input) => input.pipe((0, rxjs_1.filter)(event => predicate(event)))
        });
        return this;
    }
    /**
     * Groups events by a key
     */
    keyBy(keySelector) {
        // Implementation would use groupBy operator
        return this;
    }
    /**
     * Applies windowing
     */
    window(type, size, slide) {
        const windowConfig = {
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
    count() {
        // Implementation would use count operator
        return this;
    }
    /**
     * Calculates sum of a field in windows
     */
    sum(fieldSelector) {
        // Implementation would use reduce operator
        return this;
    }
    /**
     * Calculates average of a field in windows
     */
    avg(fieldSelector) {
        // Implementation would use reduce operator
        return this;
    }
    /**
     * Sets the sink for the pipeline
     */
    to(sink) {
        this.processor.addSink(sink);
        return this;
    }
    /**
     * Starts the pipeline
     */
    start() {
        this.processor.start();
    }
    /**
     * Stops the pipeline
     */
    stop() {
        this.processor.stop();
    }
}
exports.StreamDSL = StreamDSL;
//# sourceMappingURL=StreamDSL.js.map