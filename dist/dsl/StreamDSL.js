"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.StreamDSL = void 0;
const rxjs_1 = require("rxjs");
const StreamProcessor_1 = require("../core/StreamProcessor");
class StreamDSL {
    constructor(processor) {
        this.processor = processor;
    }
    // Map operation
    map(fn) {
        this.processor.addTransformation({
            transform: (input) => input.pipe((0, rxjs_1.map)(fn))
        });
        return this;
    }
    // Filter operation
    where(predicate) {
        this.processor.addFilter({
            filter: (input) => input.pipe((0, rxjs_1.filter)(predicate))
        });
        return this;
    }
    // Window operation
    window(config) {
        this.processor.addAggregator({
            aggregate: (input) => input
        }, config);
        return this;
    }
    // Count operation
    count(windowConfig) {
        return this.aggregate(windowConfig, () => 0, (acc) => acc + 1, (count) => StreamProcessor_1.StreamProcessor.createEvent('count', { count }));
    }
    // Sum operation
    sum(field, windowConfig) {
        return this.aggregate(windowConfig, () => 0, (acc, event) => acc + event.payload[field], (sum) => StreamProcessor_1.StreamProcessor.createEvent('sum', { sum, field }));
    }
    // Average operation
    avg(field, windowConfig) {
        let count = 0;
        return this.aggregate(windowConfig, () => ({ sum: 0, count: 0 }), (acc, event) => {
            count++;
            return {
                sum: acc.sum + event.payload[field],
                count: count
            };
        }, (result) => StreamProcessor_1.StreamProcessor.createEvent('average', {
            average: result.sum / result.count,
            field
        }));
    }
    // Generic aggregate operation
    aggregate(windowConfig, initial, accumulator, resultSelector) {
        let state = initial();
        this.processor.addAggregator({
            aggregate: (input) => new rxjs_1.Observable(subscriber => {
                const subscription = input.subscribe({
                    next: (event) => {
                        state = accumulator(state, event);
                        subscriber.next(resultSelector(state));
                    },
                    error: (err) => subscriber.error(err),
                    complete: () => subscriber.complete()
                });
                return subscription;
            })
        }, windowConfig);
        return this;
    }
    // Start processing
    async start() {
        await this.processor.start();
    }
    // Stop processing
    stop() {
        this.processor.stop();
    }
}
exports.StreamDSL = StreamDSL;
//# sourceMappingURL=StreamDSL.js.map