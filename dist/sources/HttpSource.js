"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.HttpSource = void 0;
const rxjs_1 = require("rxjs");
const http = __importStar(require("http"));
const uuid_1 = require("uuid");
/**
 * HTTP source that listens for incoming requests
 */
class HttpSource {
    constructor(options) {
        this.events$ = new rxjs_1.Subject();
        this.port = options.port;
        this.path = options.path || '/events';
        this.method = options.method || 'POST';
        this.server = http.createServer(this.handleRequest.bind(this));
    }
    /**
     * Starts the HTTP server
     */
    start() {
        return new Promise((resolve, reject) => {
            this.server.listen(this.port, () => {
                console.log(`HTTP source listening on port ${this.port}`);
                resolve();
            });
            this.server.on('error', reject);
        });
    }
    /**
     * Stops the HTTP server
     */
    stop() {
        return new Promise((resolve, reject) => {
            this.server.close((err) => {
                if (err) {
                    reject(err);
                }
                else {
                    this.events$.complete();
                    resolve();
                }
            });
        });
    }
    /**
     * Handles incoming HTTP requests
     */
    handleRequest(req, res) {
        if (req.url === this.path && req.method === this.method) {
            let body = '';
            req.on('data', chunk => {
                body += chunk.toString();
            });
            req.on('end', () => {
                try {
                    const payload = JSON.parse(body);
                    const event = {
                        id: (0, uuid_1.v4)(),
                        timestamp: Date.now(),
                        type: 'http',
                        payload
                    };
                    this.events$.next(event);
                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ status: 'success' }));
                }
                catch (error) {
                    res.writeHead(400, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ error: 'Invalid JSON' }));
                }
            });
        }
        else {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Not Found' }));
        }
    }
    /**
     * Returns an observable of events
     */
    read() {
        return this.events$.asObservable();
    }
    /**
     * Completes the event stream
     */
    complete() {
        this.events$.complete();
    }
}
exports.HttpSource = HttpSource;
//# sourceMappingURL=HttpSource.js.map