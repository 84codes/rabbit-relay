import { AMQPChannel } from "@cloudamqp/amqp-client";
import { amqpReplyCode } from "./amqpOptions.js";
import { TopologyPlan } from "./topologyPlan.js";

export type TopologyValidationIssueType =
  | "missing_exchange"
  | "missing_queue"
  | "validation_error"
  | "binding_not_validated";

export interface TopologyValidationIssue {
  type: TopologyValidationIssueType;
  exchange?: string;
  queue?: string;
  routingKey?: string;
  message: string;
  error?: unknown;
}

export interface TopologyValidationResult {
  valid: boolean;
  issues: TopologyValidationIssue[];
}

function getErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === "string") return err;

  try {
    return JSON.stringify(err);
  } catch {
    return "Unknown validation error";
  }
}

/**
 * Validate planned topology using passive AMQP checks.
 *
 * This method is intentionally non-invasive:
 * - checks exchanges exist
 * - checks queues exist
 * - does not declare anything
 * - does not modify bindings
 *
 * Binding validation is reported as informational because AMQP does not expose
 * a simple passive binding check.
 */
export async function validateTopologyPlan(
  channel: AMQPChannel,
  plan: TopologyPlan
): Promise<TopologyValidationResult> {
  const issues: TopologyValidationIssue[] = [];

  for (const exchange of plan.exchanges) {
    try {
      await channel.exchangeDeclare(exchange.name, exchange.type, {
        passive: true,
      });
    } catch (err) {
      const code = amqpReplyCode(err);

      if (code === 404) {
        issues.push({
          type: "missing_exchange",
          exchange: exchange.name,
          message: `Exchange '${exchange.name}' does not exist`,
          error: err,
        });
      } else {
        issues.push({
          type: "validation_error",
          exchange: exchange.name,
          message: `Failed to validate exchange '${exchange.name}': ${getErrorMessage(err)}`,
          error: err,
        });
      }
    }
  }

  for (const queue of plan.queues) {
    try {
      await channel.queueDeclare(queue.name, { passive: true });
    } catch (err) {
      const code = amqpReplyCode(err);

      if (code === 404) {
        issues.push({
          type: "missing_queue",
          queue: queue.name,
          message: `Queue '${queue.name}' does not exist`,
          error: err,
        });
      } else {
        issues.push({
          type: "validation_error",
          queue: queue.name,
          message: `Failed to validate queue '${queue.name}': ${getErrorMessage(err)}`,
          error: err,
        });
      }
    }
  }

  for (const binding of plan.bindings) {
    issues.push({
      type: "binding_not_validated",
      queue: binding.queue,
      exchange: binding.exchange,
      routingKey: binding.routingKey,
      message:
        `Binding '${binding.queue}' -> '${binding.exchange}' with routing key ` +
        `'${binding.routingKey}' was included in the plan but not passively validated. ` +
        `AMQP does not expose a safe binding check.`,
    });
  }

  return {
    valid: issues.every((issue) => issue.type === "binding_not_validated"),
    issues,
  };
}
