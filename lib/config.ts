import { AMQPChannel, AMQPClient } from "@cloudamqp/amqp-client";
import os from "node:os";

export const rabbitMQUrl =
  process.env.RABBITMQ_URL ?? "amqp://user:password@localhost";

export type Connector = (url: string) => AMQPClient;

export type RabbitMQConnectionManagerOptions = {
  url?: string;
  connectionName?: string;
  connector?: Connector;
};

/** amqp-client.js reads the connection name from the URL's `name` parameter. */
function withConnectionName(url: string, connectionName: string): string {
  const parsed = new URL(url);
  parsed.searchParams.set("name", connectionName);
  return parsed.toString();
}

export class RabbitMQConnectionManager {
  private readonly url: string;
  private readonly connectionName: string;
  private readonly connector: Connector;

  private connection: AMQPClient | null = null;
  private connectionOpening: Promise<AMQPClient> | null = null;
  private channel: AMQPChannel | null = null;
  private channelOpening: Promise<AMQPChannel> | null = null;
  private confirmChannel: AMQPChannel | null = null;
  private confirmChannelOpening: Promise<AMQPChannel> | null = null;
  private isolatedConnections = new Set<AMQPClient>();
  private closed = false;
  private closePromise: Promise<void> | undefined;

  constructor(options: RabbitMQConnectionManagerOptions = {}) {
    this.url = options.url ?? rabbitMQUrl;
    this.connectionName =
      options.connectionName ??
      process.env.AMQP_CONN_NAME ??
      `app:${process.title || "node"}@${os.hostname()}#${process.pid}`;
    this.connector = options.connector ?? ((url: string) => new AMQPClient(url));
  }

  private attachConnectionHandlers(connection: AMQPClient): void {
    connection.onblocked = (reason) =>
      console.warn("[amqp] connection blocked:", reason);

    connection.onunblocked = () => console.log("[amqp] connection unblocked");

    connection.ondisconnect = (error) => {
      if (this.connection === connection) {
        this.connection = null;
        this.connectionOpening = null;
        this.channel = null;
        this.channelOpening = null;
        this.confirmChannel = null;
        this.confirmChannelOpening = null;
      }

      if (error && !this.closed) {
        console.error("[amqp] connection error:", error);
      }
    };
  }

  private trackChannel(channel: AMQPChannel, kind: "regular" | "confirm"): void {
    const propagate = channel.onerror;

    channel.onerror = (reason: string) => {
      if (kind === "regular" && this.channel === channel) {
        this.channel = null;
        this.channelOpening = null;
      }

      if (kind === "confirm" && this.confirmChannel === channel) {
        this.confirmChannel = null;
        this.confirmChannelOpening = null;
      }

      if (!this.closed) {
        console.error(
          `[amqp] ${kind === "confirm" ? "confirm " : ""}channel error:`,
          reason
        );
      }

      propagate(reason);
    };
  }

  public async getConnection(): Promise<AMQPClient> {
    if (this.closed) {
      throw new Error("RabbitMQ connection manager is closed");
    }

    if (this.connection && !this.connection.closed) return this.connection;
    if (this.connectionOpening) return this.connectionOpening;

    this.connectionOpening = (async () => {
      try {
        const connection = this.connector(
          withConnectionName(this.url, this.connectionName)
        );
        this.attachConnectionHandlers(connection);
        await connection.connect();

        if (this.closed) {
          await connection.close().catch(() => undefined);
          throw new Error("RabbitMQ connection manager is closed");
        }

        this.connection = connection;
        return connection;
      } catch (error) {
        this.connection = null;
        throw error;
      } finally {
        this.connectionOpening = null;
      }
    })();

    return this.connectionOpening;
  }

  public async getChannel(): Promise<AMQPChannel> {
    if (this.closed) {
      throw new Error("RabbitMQ connection manager is closed");
    }

    if (this.channel && !this.channel.closed) return this.channel;
    if (this.channelOpening) return this.channelOpening;

    this.channelOpening = (async () => {
      try {
        const connection = await this.getConnection();
        const channel = await connection.channel();

        if (this.closed) {
          await channel.close().catch(() => undefined);
          throw new Error("RabbitMQ connection manager is closed");
        }

        this.trackChannel(channel, "regular");
        this.channel = channel;
        return channel;
      } catch (error) {
        this.channel = null;
        throw error;
      } finally {
        this.channelOpening = null;
      }
    })();

    return this.channelOpening;
  }

  public async getConfirmChannel(): Promise<AMQPChannel> {
    if (this.closed) {
      throw new Error("RabbitMQ connection manager is closed");
    }

    if (this.confirmChannel && !this.confirmChannel.closed) {
      return this.confirmChannel;
    }

    if (this.confirmChannelOpening) return this.confirmChannelOpening;

    this.confirmChannelOpening = (async () => {
      try {
        const connection = await this.getConnection();
        const channel = await connection.channel();
        await channel.confirmSelect();

        if (this.closed) {
          await channel.close().catch(() => undefined);
          throw new Error("RabbitMQ connection manager is closed");
        }

        this.trackChannel(channel, "confirm");
        this.confirmChannel = channel;
        return channel;
      } catch (error) {
        this.confirmChannel = null;
        throw error;
      } finally {
        this.confirmChannelOpening = null;
      }
    })();

    return this.confirmChannelOpening;
  }

  public async createChannel(): Promise<AMQPChannel> {
    const connection = await this.getConnection();
    const channel = await connection.channel();

    if (this.closed) {
      await channel.close().catch(() => undefined);
      throw new Error("RabbitMQ connection manager is closed");
    }

    return channel;
  }

  public async createValidationSession(): Promise<{
    createChannel(): Promise<AMQPChannel>;
    close(): Promise<void>;
  }> {
    if (this.closed) {
      throw new Error("RabbitMQ connection manager is closed");
    }

    const connection = this.connector(
      withConnectionName(this.url, `${this.connectionName}:validation`)
    );
    connection.onerror = () => undefined;
    await connection.connect();

    if (this.closed) {
      await connection.close().catch(() => undefined);
      throw new Error("RabbitMQ connection manager is closed");
    }

    this.isolatedConnections.add(connection);
    let sessionClosed = false;

    const close = async (): Promise<void> => {
      if (sessionClosed) return;
      sessionClosed = true;
      this.isolatedConnections.delete(connection);
      await connection.close().catch(() => undefined);
    };

    return {
      createChannel: async (): Promise<AMQPChannel> => {
        if (this.closed || sessionClosed) {
          throw new Error("RabbitMQ validation session is closed");
        }

        const channel = await connection.channel();
        channel.onerror = () => undefined;

        if (this.closed || sessionClosed) {
          await channel.close().catch(() => undefined);
          throw new Error("RabbitMQ validation session is closed");
        }

        return channel;
      },
      close,
    };
  }

  public health() {
    return {
      connected: this.connection ? !this.connection.closed : false,
      channelOpen: this.channel ? !this.channel.closed : false,
      confirmChannelOpen: this.confirmChannel
        ? !this.confirmChannel.closed
        : false,
    };
  }

  public close(): Promise<void> {
    if (!this.closePromise) {
      this.closePromise = this.performClose();
    }

    return this.closePromise;
  }

  private async performClose(): Promise<void> {
    this.closed = true;

    const confirmChannel = this.confirmChannel;
    const channel = this.channel;
    const connection = this.connection;
    const isolatedConnections = [...this.isolatedConnections];

    this.confirmChannel = null;
    this.confirmChannelOpening = null;
    this.channel = null;
    this.channelOpening = null;
    this.connection = null;
    this.connectionOpening = null;
    this.isolatedConnections.clear();

    await Promise.all(
      isolatedConnections.map((isolated) =>
        isolated.close().catch(() => undefined)
      )
    );
    await confirmChannel?.close().catch(() => undefined);
    await channel?.close().catch(() => undefined);
    await connection?.close().catch(() => undefined);
  }
}
