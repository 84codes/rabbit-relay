import { AMQPChannel } from "@cloudamqp/amqp-client";
import {
  AmqpArguments,
  AmqpExchangeOptions,
  AmqpQueueOptions,
  amqpReplyCode,
  exchangeArgumentsFrom,
  queueArgumentsFrom,
} from "./amqpOptions.js";
import {
  DeadLetterConfig,
  ExchangeConfig,
  InternalCfg,
  QueueConfig,
  TopologyMode,
} from "./types.js";
import {
  TopologyBindingPlan,
  TopologyExchangePlan,
  TopologyPlan,
  TopologyQueuePlan,
} from "./topologyPlan.js";

function mergeArguments(
  ...args: Array<AmqpArguments | undefined>
): AmqpArguments | undefined {
  const merged = Object.assign({}, ...args.filter(Boolean));
  return Object.keys(merged).length > 0 ? merged : undefined;
}

function mergeBindArguments(
  ...args: Array<AmqpArguments | undefined>
): AmqpArguments | undefined {
  const merged = Object.assign({}, ...args.filter(Boolean));
  return Object.keys(merged).length > 0 ? merged : undefined;
}

function toRecord(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object") return undefined;

  const obj = value as Record<string, unknown>;
  const clean: Record<string, unknown> = {};

  for (const [key, val] of Object.entries(obj)) {
    if (val !== undefined) {
      clean[key] = val;
    }
  }

  return Object.keys(clean).length > 0 ? clean : undefined;
}

function omitKeys<T extends Record<string, unknown>>(
  value: T | undefined,
  keys: string[]
): Record<string, unknown> | undefined {
  if (!value) return undefined;

  const clean: Record<string, unknown> = {};

  for (const [key, val] of Object.entries(value)) {
    if (!keys.includes(key) && val !== undefined) {
      clean[key] = val;
    }
  }

  return Object.keys(clean).length > 0 ? clean : undefined;
}

export function resolveTopologyMode(
  mode: TopologyMode | undefined
): TopologyMode {
  if (mode == null) return "assert";

  if (mode === "assert" || mode === "passive" || mode === "plan-only") {
    return mode;
  }

  throw new Error(
    `[broker] invalid topologyMode '${String(mode)}'. ` +
      `Expected one of: "assert", "passive", "plan-only".`
  );
}

/**
 * Extract the specific inequivalent argument/attribute and resource name from
 * a 406 PRECONDITION_FAILED channel error so we can give actionable advice.
 *
 * The broker reply text looks like, e.g.:
 *   PRECONDITION_FAILED - inequivalent arg 'x-dead-letter-exchange' for queue
 *   'tasks.q' in vhost '/': received 'none' but current is the value 'dlx' of
 *   type 'longstr'
 *   PRECONDITION_FAILED - inequivalent arg 'type' for exchange 'orders.ex' in
 *   vhost '/': received 'topic' but current is the value 'direct' of type 'longstr'
 */
function extractPreconditionDetail(
  err: unknown
): { attribute: string; resourceType: string; resourceName: string } | null {
  if (!err || typeof err !== "object") return null;

  const msg = typeof (err as any).message === "string" ? (err as any).message : "";

  const argMatch = msg.match(
    /inequivalent arg '([^']+)' for (queue|exchange) '([^']+)'/
  );

  if (argMatch) {
    return {
      attribute: argMatch[1],
      resourceType: argMatch[2],
      resourceName: argMatch[3],
    };
  }

  return null;
}

function buildPreconditionMessage(
  detail: { attribute: string; resourceType: string; resourceName: string } | null,
  fallbackResourceName: string,
  resourceType: "queue" | "exchange"
): string {
  const name = detail?.resourceName ?? fallbackResourceName;
  const attribute = detail?.attribute;

  const what = attribute
    ? `because it declares a different value for '${attribute}'`
    : `because its arguments/options differ`;

  return (
    `[broker] ${resourceType === "queue" ? "Queue" : "Exchange"} '${name}' already exists in RabbitMQ with different properties ` +
    `${what}. This usually happens when two processes declare the same ` +
    `${resourceType} with mismatched arguments.` +
    `\n  Fix: match the existing ${resourceType} arguments, switch to { topologyMode: "passive" } ` +
    `for one process, or delete the existing ${resourceType} first.`
  );
}

function buildDeadLetterQueueArguments(
  deadLetter?: DeadLetterConfig
): AmqpArguments | undefined {
  if (!deadLetter) return undefined;

  return {
    "x-dead-letter-exchange": deadLetter.exchange,
    ...(deadLetter.routingKey
      ? { "x-dead-letter-routing-key": deadLetter.routingKey }
      : {}),
  };
}

async function assertDeadLetterTopology(params: {
  channel: AMQPChannel;
  durable: boolean;
  deadLetter?: DeadLetterConfig;
}) {
  const { channel, durable, deadLetter } = params;

  if (!deadLetter || !deadLetter.autoDeclare) return;

  if (!deadLetter.queue) {
    throw new Error(
      "[broker] deadLetter.queue is required when deadLetter.autoDeclare=true"
    );
  }

  const exchangeType = deadLetter.exchangeType ?? "topic";
  const routingKey = deadLetter.routingKey ?? "#";

  const exchangeOptions: AmqpExchangeOptions = {
    durable,
    ...(deadLetter.exchangeOptions ?? {}),
  };

  await channel.exchangeDeclare(
    deadLetter.exchange,
    exchangeType,
    exchangeOptions,
    exchangeArgumentsFrom(exchangeOptions)
  );

  const queueOptions: AmqpQueueOptions = {
    durable,
    ...(deadLetter.queueOptions ?? {}),
  };

  await channel.queueDeclare(
    deadLetter.queue,
    queueOptions,
    queueArgumentsFrom(queueOptions)
  );

  await channel.queueBind(
    deadLetter.queue,
    deadLetter.exchange,
    routingKey,
    deadLetter.bindArguments ?? {}
  );
}

export function mergeInternalCfg(
  defaultCfg: InternalCfg,
  exchangeConfig: ExchangeConfig
): InternalCfg {
  return {
    exchangeType: exchangeConfig.exchangeType ?? defaultCfg.exchangeType,
    routingKey: exchangeConfig.routingKey ?? defaultCfg.routingKey,
    durable: exchangeConfig.durable ?? defaultCfg.durable,
    publisherConfirms:
      exchangeConfig.publisherConfirms ?? defaultCfg.publisherConfirms,
    binding: exchangeConfig.binding ?? defaultCfg.binding,
    queueArgs: exchangeConfig.queueArgs ?? defaultCfg.queueArgs,
    topologyMode: resolveTopologyMode(
      exchangeConfig.topologyMode ?? defaultCfg.topologyMode
    ),
    maxMessageBytes: exchangeConfig.maxMessageBytes ?? defaultCfg.maxMessageBytes,
    passiveQueue: exchangeConfig.passiveQueue ?? defaultCfg.passiveQueue,
    deadLetter: exchangeConfig.deadLetter ?? defaultCfg.deadLetter,
    amqp: {
      exchange: {
        ...(defaultCfg.amqp?.exchange ?? {}),
        ...(exchangeConfig.amqp?.exchange ?? {}),
      },
      queue: {
        ...(defaultCfg.amqp?.queue ?? {}),
        ...(exchangeConfig.amqp?.queue ?? {}),
      },
      bind: {
        ...(defaultCfg.amqp?.bind ?? {}),
        ...(exchangeConfig.amqp?.bind ?? {}),
      },
    },
  };
}

export function createTopologyPlan(params: {
  exchangeName: string;
  queueName?: string;
  queueConfig?: QueueConfig;
  defaultCfg: InternalCfg;
  exchangeConfig: ExchangeConfig;
}): TopologyPlan {
  const { exchangeName, queueName, queueConfig, defaultCfg, exchangeConfig } =
    params;

  const cfg = mergeInternalCfg(defaultCfg, exchangeConfig);

  const exchanges: TopologyExchangePlan[] = [];
  const queues: TopologyQueuePlan[] = [];
  const bindings: TopologyBindingPlan[] = [];

  const exchangeOptions: AmqpExchangeOptions = {
    durable: cfg.durable,
    ...(cfg.amqp?.exchange ?? {}),
  };

  exchanges.push({
    name: exchangeName,
    type: cfg.exchangeType,
    durable: cfg.durable,
    options: omitKeys(
      exchangeOptions as Record<string, unknown>,
      ["durable"]
    ),
  });

  if (cfg.deadLetter) {
    if (cfg.deadLetter.autoDeclare && !cfg.deadLetter.queue) {
      throw new Error(
        "[broker] deadLetter.queue is required when deadLetter.autoDeclare=true"
      );
    }

    const dlxType = cfg.deadLetter.exchangeType ?? "topic";
    const dlqRoutingKey = cfg.deadLetter.routingKey ?? "#";

    exchanges.push({
      name: cfg.deadLetter.exchange,
      type: dlxType,
      durable: cfg.durable,
      options: toRecord(cfg.deadLetter.exchangeOptions),
    });

    if (cfg.deadLetter.queue) {
      queues.push({
        name: cfg.deadLetter.queue,
        durable: cfg.durable,
        arguments: toRecord(cfg.deadLetter.queueOptions?.arguments),
        options: omitKeys(
          cfg.deadLetter.queueOptions as Record<string, unknown> | undefined,
          ["durable", "arguments"]
        ),
      });

      bindings.push({
        queue: cfg.deadLetter.queue,
        exchange: cfg.deadLetter.exchange,
        routingKey: dlqRoutingKey,
        arguments: toRecord(cfg.deadLetter.bindArguments),
      });
    }
  }

  const queueAmqpOptions = {
    ...(cfg.amqp?.queue ?? {}),
    ...(queueConfig?.amqp?.queue ?? {}),
  } as AmqpQueueOptions;

  const deadLetterArgs = buildDeadLetterQueueArguments(cfg.deadLetter);

  const mergedArgs = mergeArguments(
    cfg.queueArgs,
    deadLetterArgs,
    cfg.amqp?.queue?.arguments,
    queueConfig?.amqp?.queue?.arguments
  );

  if (queueName) {
    queues.push({
      name: queueName,
      durable: cfg.durable,
      passive: cfg.passiveQueue || undefined,
      arguments: toRecord(mergedArgs),
      options: omitKeys(
        queueAmqpOptions as Record<string, unknown>,
        ["durable", "arguments"]
      ),
    });
  }

  const bindArgs = mergeBindArguments(cfg.amqp?.bind);

  if (queueName && cfg.binding !== false) {
    bindings.push({
      queue: queueName,
      exchange: exchangeName,
      routingKey: cfg.routingKey,
      arguments: toRecord(bindArgs),
    });
  }

  return {
    exchanges,
    queues,
    bindings,
  };
}

export function createAssertTopology(params: {
  exchangeName: string;
  queueName?: string;
  queueConfig?: QueueConfig;
  defaultCfg: InternalCfg;
  exchangeConfig: ExchangeConfig;
}) {
  const { exchangeName, queueName, queueConfig, defaultCfg, exchangeConfig } =
    params;

  return async function assertTopology(channel: AMQPChannel) {
    const cfg = mergeInternalCfg(defaultCfg, exchangeConfig);

    const exchangeOpts: AmqpExchangeOptions = {
      durable: cfg.durable,
      ...(cfg.amqp?.exchange ?? {}),
    };

    try {
      await channel.exchangeDeclare(
        exchangeName,
        cfg.exchangeType,
        exchangeOpts,
        exchangeArgumentsFrom(exchangeOpts)
      );
    } catch (err: unknown) {
      if (amqpReplyCode(err) === 406) {
        throw new Error(
          buildPreconditionMessage(
            extractPreconditionDetail(err),
            exchangeName,
            "exchange"
          )
        );
      }
      throw err;
    }

    await assertDeadLetterTopology({
      channel,
      durable: cfg.durable,
      deadLetter: cfg.deadLetter,
    });

    const queueAmqpOptions = {
      ...(cfg.amqp?.queue ?? {}),
      ...(queueConfig?.amqp?.queue ?? {}),
    } as AmqpQueueOptions;

    const deadLetterArgs = buildDeadLetterQueueArguments(cfg.deadLetter);

    if (!queueName) {
      return;
    }

    if (cfg.passiveQueue) {
      if (cfg.queueArgs || queueAmqpOptions.arguments || deadLetterArgs) {
        console.warn(
          `[broker] passiveQueue=true: ignoring queue arguments for '${queueName}' (not declaring).`
        );
      }

      try {
        await channel.queueDeclare(queueName, { passive: true });
      } catch (err: unknown) {
        if (amqpReplyCode(err) === 404) {
          throw new Error(
            `[broker] passiveQueue check failed: queue '${queueName}' does not exist. ` +
              `Either create it in your setup step with the desired arguments, ` +
              `or call with passiveQueue:false and queue options to auto-declare.`
          );
        }

        throw err;
      }
    } else {
      try {
        const mergedArgs = mergeArguments(
          cfg.queueArgs,
          deadLetterArgs,
          cfg.amqp?.queue?.arguments,
          queueConfig?.amqp?.queue?.arguments
        );

        const qOpts: AmqpQueueOptions = {
          durable: cfg.durable,
          ...queueAmqpOptions,
          ...(mergedArgs ? { arguments: mergedArgs } : {}),
        };

        await channel.queueDeclare(queueName, qOpts, queueArgumentsFrom(qOpts));
      } catch (err: unknown) {
        if (amqpReplyCode(err) === 406) {
          throw new Error(
            buildPreconditionMessage(
              extractPreconditionDetail(err),
              queueName,
              "queue"
            )
          );
        }

        throw err;
      }
    }

    if (cfg.binding !== false) {
      const bindArgs = mergeBindArguments(cfg.amqp?.bind);

      // (Re)bind is idempotent - safe to call even if binding already exists
      await channel.queueBind(
        queueName,
        exchangeName,
        cfg.routingKey,
        bindArgs ?? {}
      );
    }
  };
}
