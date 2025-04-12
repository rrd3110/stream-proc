"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const StreamDSL_1 = require("../core/StreamDSL");
const HttpSource_1 = require("../sources/HttpSource");
const ConsoleSink_1 = require("../sinks/ConsoleSink");
/**
 * Simple example demonstrating the stream processor
 */
async function main() {
    // Create an HTTP source
    const source = new HttpSource_1.HttpSource({
        port: 3000,
        path: '/events',
        method: 'POST'
    });
    // Start the HTTP server
    await source.start();
    // Create a pipeline using the DSL
    const pipeline = new StreamDSL_1.StreamDSL()
        .from(source)
        .map(event => ({
        ...event,
        payload: {
            ...event.payload,
            processed: true,
            timestamp: new Date().toISOString()
        }
    }))
        .where(event => event.payload.value > 10)
        .window('tumbling', 1000)
        .count()
        .to(new ConsoleSink_1.ConsoleSink({ format: 'pretty' }));
    // Start the pipeline
    pipeline.start();
    console.log('Stream processor started. Send POST requests to http://localhost:3000/events');
    console.log('Example payload: { "value": 15 }');
    // Handle process termination
    process.on('SIGINT', async () => {
        console.log('Stopping stream processor...');
        pipeline.stop();
        await source.stop();
        process.exit(0);
    });
}
// Run the example
main().catch(console.error);
//# sourceMappingURL=simple.js.map