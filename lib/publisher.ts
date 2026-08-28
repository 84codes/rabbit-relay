import { AMQPChannel } from "@cloudamqp/amqp-client";
import { AmqpPublishOptions, toProperties } from "./amqpOptions.js";
import { pluginManager } from "./pluginManager.js";
import { EventEnvelope, EventMeta } from "./eventFactories.js";
import {
  ExchangeConfig,
  InternalCfg,
  PublishOptions,
  RequestOptions,
} from "./types.js";
import { generateUuid } from "./uuid.js";
import { MessageTooLargeError } from "./errors.js";
import { LifecycleEmit } from "./lifecycle.js";

function buildPublishProps(
  event: EventEnvelope,
  opts?: PublishOptions
): AmqpPublishOptions {
  const nativePublish = opts?.amqp?.publish ?? {};
  const baseHeaders = event.meta?.headers ?? {};
  const nativeHeaders = nativePublish.headers ?? {};

  return {
    messageId: event.id,
    type: event.name,
    timestamp: new Date(event.time ?? Date.now()),
    correlationId: event.meta?.corrId,
    ...nativePublish,
    headers: { ...baseHeaders, ...nativeHeaders },
  };
}

function buildRpcPublishProps(
  event: EventEnvelope,
  correlationId: string,
  replyTo: string,
  opts?: PublishOptions
): AmqpPublishOptions {
  const nativePublish = opts?.amqp?.publish ?? {};
  const baseHeaders = event.meta?.headers ?? {};
  const nativeHeaders = nativePublish.headers ?? {};

  return {
    messageId: event.id,
    type: event.name,
    timestamp: new Date(event.time ?? Date.now()),
    correlationId,
    replyTo,
    ...nativePublish,
    headers: { ...baseHeaders, ...nativeHeaders },
  };
}

function assertMessageSize(
  event: EventEnvelope,
  content: Buffer,
  maxMessageBytes?: number
) {
  if (maxMessageBytes == null) return;

  if (!Number.isFinite(maxMessageBytes) || maxMessageBytes <= 0) {
    throw new Error(`[broker] maxMessageBytes must be a positive number, got ${maxMessageBytes}`);
  }

  if (content.length > maxMessageBytes) {
    throw new MessageTooLargeError({
      eventName: event.name,
      sizeBytes: content.length,
      maxBytes: maxMessageBytes,
    });
  }
}

export function createPublisher(params: {
  peerName: string;
  exchangeName: string;
  exchangeConfig: ExchangeConfig;
  defaultCfg: InternalCfg;
  getChannel: () => Promise<AMQPChannel>;
  getConfirmChannel: () => Promise<AMQPChannel>;
  getBackoffMs: () => number;
  emitLifecycle: LifecycleEmit;
}) {
  const {
    peerName,
    exchangeName,
    exchangeConfig,
    defaultCfg,
    getChannel,
    getConfirmChannel,
    getBackoffMs,
    emitLifecycle,
  } = params;

  const resolveMaxMessageBytes = (opts?: PublishOptions): number | undefined => {
    return (
      opts?.maxMessageBytes ??
      exchangeConfig.maxMessageBytes ??
      defaultCfg.maxMessageBytes
    );
  };

  const resolvePublishRoutingKey = (
    evt: EventEnvelope,
    opts?: PublishOptions
  ): string => {
    if (opts?.routingKey) return opts.routingKey;

    const configuredRoutingKey = exchangeConfig.routingKey;

    // exchangeConfig.routingKey is often a binding pattern for consumers.
    // Do not use topic wildcards as publish routing keys.
    if (
      configuredRoutingKey &&
      configuredRoutingKey !== "#" &&
      configuredRoutingKey !== "*" &&
      !configuredRoutingKey.includes("#") &&
      !configuredRoutingKey.includes("*")
    ) {
      return configuredRoutingKey;
    }

    return evt.name;
  };

  const serializeEvent = (
    event: EventEnvelope,
    opts?: PublishOptions
  ): Buffer => {
    const content = Buffer.from(JSON.stringify(event));
    assertMessageSize(event, content, resolveMaxMessageBytes(opts));
    return content;
  };

  const getPubChannel = async (): Promise<AMQPChannel> => {
    if (exchangeConfig.publisherConfirms ?? defaultCfg.publisherConfirms) {
      return getConfirmChannel();
    }

    return getChannel();
  };

  const safePublish = async (
    publish: (ch: AMQPChannel) => unknown | Promise<unknown>
  ) => {
    try {
      const ch = await getPubChannel();
      await publish(ch);
    } catch (err) {
      // Broker is likely reconnecting. Briefly wait, then retry once.
      const delay = Math.min(getBackoffMs() * 2, 2000);
      await new Promise((r) => setTimeout(r, delay));

      const ch2 = await getPubChannel();
      await publish(ch2);
    }
  };

  const publishOne = async (
    evt: EventEnvelope,
    opts?: PublishOptions
  ): Promise<void> => {
    await pluginManager.executeHook("beforeProduce", evt);

    const content = serializeEvent(evt, opts);
    const routingKey = resolvePublishRoutingKey(evt, opts);

    try {
      await safePublish((ch) => {
        const props = buildPublishProps(evt, opts);

        return ch.basicPublish(
          exchangeName,
          routingKey,
          content,
          toProperties(props),
          props.mandatory
        );
      });
    } catch (err) {
      await emitLifecycle("publish.failed", {
        peerName,
        exchange: exchangeName,
        routingKey,
        eventName: evt.name,
        error: err,
      });

      throw err;
    }

    await pluginManager.executeHook("afterProduce", evt, null);
  };

  const requestOne = async (
    evt: EventEnvelope & { meta?: EventMeta },
    opts?: PublishOptions
  ): Promise<unknown> => {
    const correlationId = generateUuid();

    const rpcCh = await getChannel();
    const temp = await rpcCh.queueDeclare("", {
      exclusive: true,
      autoDelete: true,
    });

    await pluginManager.executeHook("beforeProduce", evt);

    const content = serializeEvent(evt, opts);
    const routingKey = resolvePublishRoutingKey(evt, opts);

    try {
      await safePublish(async (pubCh) => {
        const props = buildRpcPublishProps(evt, correlationId, temp.name, opts);

        await pubCh.basicPublish(
          exchangeName,
          routingKey,
          content,
          toProperties(props),
          props.mandatory
        );
      });
    } catch (err) {
      await emitLifecycle("publish.failed", {
        peerName,
        exchange: exchangeName,
        routingKey,
        eventName: evt.name,
        error: err,
      });

      throw err;
    }

    const timeoutMs = evt.meta?.timeoutMs ?? 5000;

    return await new Promise((resolve, reject) => {
      let ctag: string | undefined;
      let settled = false;

      const cleanup = async () => {
        try {
          if (ctag) await rpcCh.basicCancel(ctag);
        } catch {}

        try {
          await rpcCh.queueDelete(temp.name);
        } catch {}
      };

      const timer = setTimeout(async () => {
        if (settled) return;

        settled = true;
        await cleanup();
        reject(new Error(`Timeout waiting for reply for event '${evt.name}' (corrId: ${correlationId})`));
      }, timeoutMs);

      rpcCh
        .basicConsume(
          temp.name,
          { noAck: true },
          (msg) => {
            if (msg.properties.correlationId !== correlationId) return;
            if (settled) return;

            settled = true;
            clearTimeout(timer);

            try {
              const reply = JSON.parse(msg.bodyToString() ?? "null").reply;
              void pluginManager.executeHook("afterProduce", evt, reply);
              resolve(reply);
            } catch (err) {
              reject(err);
            } finally {
              void cleanup();
            }
          }
        )
        .then((consumer) => {
          ctag = consumer.tag;
        })
        .catch((err) => {
          if (settled) return;

          settled = true;
          clearTimeout(timer);
          reject(err);
        });
    });
  };

  const produceMany = async <
    TEvents extends Record<string, EventEnvelope>,
    K extends keyof TEvents
  >(
    ...events: TEvents[K][]
  ): Promise<void> => {
    for (const evt of events) {
      await publishOne(evt as EventEnvelope);
    }
  };

  const produce = async <
    TEvents extends Record<string, EventEnvelope>,
    K extends keyof TEvents
  >(
    ...events: TEvents[K][]
  ): Promise<void | unknown> => {
    // Back-compat: upgrade legacy "wait" (if present) to meta fields
    if (events.length === 1 && (events[0] as any)?.wait) {
      const first: any = events[0];
      const w = first.wait as { source?: string; timeout?: number };

      first.meta = first.meta || {};

      if (first.meta.expectsReply !== true) first.meta.expectsReply = true;
      if (w?.timeout != null && first.meta.timeoutMs == null) {
        first.meta.timeoutMs = w.timeout;
      }

      if (w?.source) {
        first.meta.headers = {
          ...(first.meta.headers || {}),
          source: w.source,
        };
      }
    }

    // RPC request path
    if (events.length === 1 && (events[0] as any)?.meta?.expectsReply === true) {
      return requestOne(events[0] as EventEnvelope & { meta?: EventMeta });
    }

    return produceMany<TEvents, K>(...events);
  };

  const publish = async <
    TEvents extends Record<string, EventEnvelope>,
    K extends keyof TEvents
  >(
    event: TEvents[K],
    opts?: PublishOptions
  ): Promise<void | unknown> => {
    if ((event as any)?.meta?.expectsReply === true) {
      return requestOne(event as EventEnvelope & { meta?: EventMeta }, opts);
    }

    return publishOne(event as EventEnvelope, opts);
  };

  const request = async <TReply = unknown>(
    event: EventEnvelope,
    opts?: RequestOptions
  ): Promise<TReply> => {
    const evt = event as EventEnvelope & { meta?: EventMeta };

    evt.meta = {
      ...(evt.meta ?? {}),
      expectsReply: true,
      ...(opts?.timeoutMs != null ? { timeoutMs: opts.timeoutMs } : {}),
    };

    return requestOne(evt, opts) as Promise<TReply>;
  };

  return { produce, produceMany, publish, request };
}
