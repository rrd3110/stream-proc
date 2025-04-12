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
class HttpSource {
    constructor(config) {
        this.server = null;
        this.config = config;
        this.events$ = new rxjs_1.Observable((subscriber) => {
            this.server = http.createServer((req, res) => {
                if (req.url === this.config.path && req.method === 'POST') {
                    let data = '';
                    req.on('data', (chunk) => {
                        data += chunk;
                    });
                    req.on('end', () => {
                        try {
                            const payload = JSON.parse(data);
                            const event = {
                                id: Math.random().toString(36).substring(7),
                                timestamp: Date.now(),
                                type: this.config.eventType,
                                payload
                            };
                            subscriber.next(event);
                            res.writeHead(200, { 'Content-Type': 'application/json' });
                            res.end(JSON.stringify({ status: 'success' }));
                        }
                        catch (error) {
                            res.writeHead(400, { 'Content-Type': 'application/json' });
                            res.end(JSON.stringify({
                                status: 'error',
                                message: 'Invalid JSON payload'
                            }));
                        }
                    });
                }
                else {
                    res.writeHead(404);
                    res.end();
                }
            });
            this.server.listen(this.config.port);
            // Cleanup when unsubscribed
            return () => {
                if (this.server) {
                    this.server.close();
                    this.server = null;
                }
            };
        });
    }
    read() {
        return this.events$;
    }
    stop() {
        if (this.server) {
            this.server.close();
            this.server = null;
        }
    }
}
exports.HttpSource = HttpSource;
//# sourceMappingURL=HttpSource.js.map