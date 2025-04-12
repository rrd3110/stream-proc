"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConsoleSink = void 0;
/**
 * Console sink that writes events to the console
 */
class ConsoleSink {
    constructor(options = {}) {
        this.format = options.format || 'pretty';
    }
    /**
     * Writes events to the console
     */
    write(data) {
        return new Promise((resolve, reject) => {
            data.subscribe({
                next: (event) => {
                    if (this.format === 'json') {
                        console.log(JSON.stringify(event));
                    }
                    else {
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
    complete() {
        console.log('Console sink completed');
    }
}
exports.ConsoleSink = ConsoleSink;
//# sourceMappingURL=ConsoleSink.js.map