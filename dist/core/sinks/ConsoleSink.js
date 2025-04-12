"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConsoleSink = void 0;
class ConsoleSink {
    constructor(config = {}) {
        this.config = {
            prefix: '📊 ',
            formatter: (event) => JSON.stringify(event, null, 2),
            ...config
        };
    }
    async write(data) {
        return new Promise((resolve, reject) => {
            const subscription = data.subscribe({
                next: (event) => {
                    const output = this.config.formatter(event);
                    console.log(`${this.config.prefix}${output}`);
                },
                error: (error) => {
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
exports.ConsoleSink = ConsoleSink;
//# sourceMappingURL=ConsoleSink.js.map