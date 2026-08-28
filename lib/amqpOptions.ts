import type { AMQPProperties, Field } from "@cloudamqp/amqp-client";

export type { Field };

export type AmqpArguments = Record<string, Field>;

/**
 * Options for declaring a queue.
 *
 * The `x-*` shorthands (messageTtl, expires, ...) are folded into queue
 * arguments before the declare is sent.
 */
export interface AmqpQueueOptions {
  exclusive?: boolean;
  durable?: boolean;
  autoDelete?: boolean;
  arguments?: AmqpArguments;
  messageTtl?: number;
  expires?: number;
  deadLetterExchange?: string;
  deadLetterRoutingKey?: string;
  maxLength?: number;
  maxPriority?: number;
}

/** Options for declaring an exchange. */
export interface AmqpExchangeOptions {
  durable?: boolean;
  internal?: boolean;
  autoDelete?: boolean;
  alternateExchange?: string;
  arguments?: AmqpArguments;
}

/** Options for publishing a single message. */
export interface AmqpPublishOptions extends Omit<AMQPProperties, "timestamp"> {
  /** Seconds since the epoch, or a Date. */
  timestamp?: Date | number;
  /** Sets deliveryMode to 2 so the broker persists the message. */
  persistent?: boolean;
  /** Return the message to the publisher if it cannot be routed. */
  mandatory?: boolean;
}

/** Options for starting a consumer. */
export interface AmqpConsumeOptions {
  consumerTag?: string;
  noAck?: boolean;
  exclusive?: boolean;
  priority?: number;
  arguments?: AmqpArguments;
}

const QUEUE_ARGUMENT_SHORTHANDS: Array<[keyof AmqpQueueOptions, string]> = [
  ["messageTtl", "x-message-ttl"],
  ["expires", "x-expires"],
  ["deadLetterExchange", "x-dead-letter-exchange"],
  ["deadLetterRoutingKey", "x-dead-letter-routing-key"],
  ["maxLength", "x-max-length"],
  ["maxPriority", "x-max-priority"],
];

function compact(args: AmqpArguments): AmqpArguments | undefined {
  const clean: AmqpArguments = {};

  for (const [key, value] of Object.entries(args)) {
    if (value !== undefined) clean[key] = value;
  }

  return Object.keys(clean).length > 0 ? clean : undefined;
}

export function queueArgumentsFrom(
  options: AmqpQueueOptions | undefined,
  extra?: AmqpArguments
): AmqpArguments {
  const args: AmqpArguments = { ...(options?.arguments ?? {}), ...(extra ?? {}) };

  for (const [option, argument] of QUEUE_ARGUMENT_SHORTHANDS) {
    const value = options?.[option];
    if (value !== undefined) args[argument] = value as Field;
  }

  return compact(args) ?? {};
}

export function exchangeArgumentsFrom(
  options: AmqpExchangeOptions | undefined
): AmqpArguments {
  const args: AmqpArguments = { ...(options?.arguments ?? {}) };

  if (options?.alternateExchange !== undefined) {
    args["alternate-exchange"] = options.alternateExchange;
  }

  return compact(args) ?? {};
}

export function toProperties(options: AmqpPublishOptions = {}): AMQPProperties {
  const { persistent, mandatory: _mandatory, timestamp, ...properties } = options;

  const resolved: AMQPProperties = { ...properties };

  if (persistent !== undefined && resolved.deliveryMode === undefined) {
    resolved.deliveryMode = persistent ? 2 : 1;
  }

  if (timestamp !== undefined) {
    resolved.timestamp =
      timestamp instanceof Date ? timestamp : new Date(timestamp * 1000);
  }

  for (const key of Object.keys(resolved) as Array<keyof AMQPProperties>) {
    if (resolved[key] === undefined) delete resolved[key];
  }

  return resolved;
}

export function toConsumeParams(options: AmqpConsumeOptions = {}) {
  const args: AmqpArguments = { ...(options.arguments ?? {}) };

  if (options.priority !== undefined) args["x-priority"] = options.priority;

  return {
    tag: options.consumerTag ?? "",
    noAck: options.noAck ?? false,
    exclusive: options.exclusive ?? false,
    args,
  };
}

/**
 * AMQPError carries the reply code in its message tail, e.g.
 * "channel 1 closed: NOT_FOUND - no queue 'q' in vhost '/' (404)".
 */
export function amqpReplyCode(err: unknown): number | undefined {
  if (!err || typeof err !== "object") return undefined;

  const candidate = err as { code?: unknown; replyCode?: unknown; message?: unknown };

  if (typeof candidate.code === "number") return candidate.code;
  if (typeof candidate.replyCode === "number") return candidate.replyCode;

  if (typeof candidate.message !== "string") return undefined;

  const match = candidate.message.match(/\((\d{3})\)\s*$/);

  return match ? Number(match[1]) : undefined;
}
