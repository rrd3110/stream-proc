"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WindowedBloomFilter = void 0;
const bloomfilter_1 = require("bloomfilter");
class WindowedBloomFilter {
    constructor(windowSize, expectedItems, errorRate = 0.01) {
        this.filters = new Map();
        this.windowSize = windowSize;
        this.expectedItems = expectedItems;
        this.errorRate = errorRate;
    }
    // Add an item to the current time window
    add(item) {
        const currentWindow = this.getCurrentWindow();
        let filter = this.filters.get(currentWindow);
        if (!filter) {
            filter = new bloomfilter_1.BloomFilter(this.expectedItems, this.errorRate);
            this.filters.set(currentWindow, filter);
        }
        filter.add(item);
        this.cleanup();
    }
    // Test if an item might exist in any active window
    test(item) {
        for (const filter of this.filters.values()) {
            if (filter.test(item)) {
                return true;
            }
        }
        return false;
    }
    // Get the current time window
    getCurrentWindow() {
        return Math.floor(Date.now() / this.windowSize);
    }
    // Remove expired windows
    cleanup() {
        const currentWindow = this.getCurrentWindow();
        for (const [window] of this.filters) {
            if (window < currentWindow - 1) {
                this.filters.delete(window);
            }
        }
    }
    // Clear all filters
    clear() {
        this.filters.clear();
    }
    // Get the number of active windows
    getActiveWindows() {
        return this.filters.size;
    }
}
exports.WindowedBloomFilter = WindowedBloomFilter;
//# sourceMappingURL=WindowedBloomFilter.js.map