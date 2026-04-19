/** Simple FIFO semaphore. No deps. */
export class Semaphore {
  private available: number;
  private readonly waiters: (() => void)[] = [];

  public constructor(permits: number) {
    if (permits <= 0) throw new Error(`permits must be > 0, got ${permits}`);
    this.available = permits;
  }

  public acquire(): Promise<void> {
    if (this.available > 0) {
      this.available--;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => this.waiters.push(resolve));
  }

  public release(): void {
    const next = this.waiters.shift();
    if (next !== undefined) {
      next();
      return;
    }
    this.available++;
  }
}
