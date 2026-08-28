import { AMQPChannel } from "@cloudamqp/amqp-client";

export class ReconnectController {
  /** The current live channel promise (created lazily and replaced after reconnect). */
  private channelPromise: Promise<AMQPChannel> | null = null;

  /** Reconnect state */
  private reconnecting = false;
  private closed = false;
  private backoffMs = 500;
  private readonly maxBackoffMs = 20000;

  /** Callbacks to run after a successful reconnect (like re-assert topology, resume consume). */
  private onReconnectCbs: Array<(ch: AMQPChannel) => void | Promise<void>> = [];
  private readonly openChannel: () => Promise<AMQPChannel>;

  constructor(openChannel: () => Promise<AMQPChannel>) {
    this.openChannel = openChannel;
  }

  public async initChannel() {
    if (this.closed) return;

    this.channelPromise = this.openChannel();
    const ch = await this.channelPromise;

    this.backoffMs = 500;

    // amqp-client.js channels are not event emitters: onerror fires for both
    // server-side channel closes and connection loss.
    const propagate = ch.onerror;

    ch.onerror = (reason: string) => {
      propagate(reason);
      void this.recover(`channel.error: ${reason}`);
    };
  }

  public getBackoffMs() {
    return this.backoffMs;
  }

  public isReconnecting() {
    return this.reconnecting;
  }

  public onReconnect(cb: (ch: AMQPChannel) => void | Promise<void>) {
    this.onReconnectCbs.push(cb);
  }

  public async getChannel(): Promise<AMQPChannel> {
    if (this.closed) {
      throw new Error("[broker] RabbitMQ broker is closed");
    }

    if (!this.channelPromise) {
      await this.initChannel();
    }

    const channel = await this.channelPromise!;

    // A channel closed by the application raises no error, so there is nothing
    // to recover from eagerly. Reopen it on next use instead.
    if (channel.closed) {
      await this.initChannel();
      return this.channelPromise!;
    }

    return channel;
  }

  public close() {
    this.closed = true;
    this.reconnecting = false;
    this.channelPromise = null;
    this.onReconnectCbs = [];
  }

  /** Reopen the channel and replay topology and consumers, with backoff. */
  public async recover(reason: string) {
    if (this.closed || this.reconnecting) return;

    this.reconnecting = true;

    while (!this.closed) {
      try {
        const jitter = Math.floor(Math.random() * 250);
        await new Promise((r) => setTimeout(r, this.backoffMs + jitter));

        if (this.closed) return;

        await this.initChannel();
        const ch = await this.channelPromise!;

        this.backoffMs = 500;
        this.reconnecting = false;

        for (const cb of this.onReconnectCbs) {
          try {
            await cb(ch);
          } catch (e) {
            console.error("[broker] onReconnect callback failed:", e);
          }
        }

        return;
      } catch {
        this.channelPromise = null;

        this.backoffMs = Math.min(
          this.maxBackoffMs,
          Math.floor(this.backoffMs * 1.7 + Math.random() * 100)
        );

        console.error(`[broker] reconnect failed (${reason}), retrying in ~${this.backoffMs}ms`);
      }
    }
  }

  public isClosed() {
    return this.closed;
  }
}
