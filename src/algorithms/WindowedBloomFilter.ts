import { BloomFilter } from 'bloomfilter';

export class WindowedBloomFilter {
  private filters: Map<number, BloomFilter>;
  private readonly windowSize: number;
  private readonly expectedItems: number;
  private readonly errorRate: number;

  constructor(windowSize: number, expectedItems: number, errorRate: number = 0.01) {
    this.filters = new Map();
    this.windowSize = windowSize;
    this.expectedItems = expectedItems;
    this.errorRate = errorRate;
  }

  // Add an item to the current time window
  public add(item: string | Buffer): void {
    const currentWindow = this.getCurrentWindow();
    let filter = this.filters.get(currentWindow);

    if (!filter) {
      filter = new BloomFilter(
        this.expectedItems,
        this.errorRate
      );
      this.filters.set(currentWindow, filter);
    }

    filter.add(item);
    this.cleanup();
  }

  // Test if an item might exist in any active window
  public test(item: string | Buffer): boolean {
    for (const filter of this.filters.values()) {
      if (filter.test(item)) {
        return true;
      }
    }
    return false;
  }

  // Get the current time window
  private getCurrentWindow(): number {
    return Math.floor(Date.now() / this.windowSize);
  }

  // Remove expired windows
  private cleanup(): void {
    const currentWindow = this.getCurrentWindow();
    for (const [window] of this.filters) {
      if (window < currentWindow - 1) {
        this.filters.delete(window);
      }
    }
  }

  // Clear all filters
  public clear(): void {
    this.filters.clear();
  }

  // Get the number of active windows
  public getActiveWindows(): number {
    return this.filters.size;
  }
} 