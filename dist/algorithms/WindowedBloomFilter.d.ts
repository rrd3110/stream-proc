export declare class WindowedBloomFilter {
    private filters;
    private readonly windowSize;
    private readonly expectedItems;
    private readonly errorRate;
    constructor(windowSize: number, expectedItems: number, errorRate?: number);
    add(item: string | Buffer): void;
    test(item: string | Buffer): boolean;
    private getCurrentWindow;
    private cleanup;
    clear(): void;
    getActiveWindows(): number;
}
