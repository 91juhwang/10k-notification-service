import { SendMessageBatchCommand, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import type { components } from "@microgrid/shared";

type TelemetryIngestRequest = components["schemas"]["TelemetryIngestRequest"];
type ControlCommandRequest = components["schemas"]["ControlCommandRequest"];

export interface TelemetryQueueMessage extends TelemetryIngestRequest {
  ingestId: string;
  queuedAt: string;
}

export interface ControlCommandQueueMessage extends ControlCommandRequest {
  commandId: string;
  requestedBy: string | null;
  createdAt: string;
}

interface PendingTelemetryMessage {
  entryId: string;
  message: TelemetryQueueMessage;
  resolve: (value: { queueMessageId: string | null }) => void;
  reject: (reason: unknown) => void;
}

interface TelemetryBatchSuccessEntry {
  Id?: string;
  MessageId?: string;
}

interface TelemetryBatchFailedEntry {
  Id?: string;
  Code?: string;
  Message?: string;
}

interface TelemetryBatchResponse {
  Successful?: TelemetryBatchSuccessEntry[];
  Failed?: TelemetryBatchFailedEntry[];
}

function requiredEnv(name: string) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }

  return value;
}

function readIntEnv(name: string, fallback: number, { min, max }: { min: number; max: number }) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }

  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }

  return parsed;
}

function createSqsClient() {
  const region = process.env.AWS_REGION ?? "us-east-1";
  const endpoint = process.env.SQS_ENDPOINT;
  const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
  const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;

  return new SQSClient({
    region,
    endpoint,
    credentials:
      accessKeyId && secretAccessKey
        ? {
            accessKeyId,
            secretAccessKey
          }
        : undefined
  });
}

const sqsClient = createSqsClient();
const telemetryQueueUrl = requiredEnv("TELEMETRY_QUEUE_URL");
const telemetryBatchSize = readIntEnv("TELEMETRY_SQS_BATCH_SIZE", 10, { min: 1, max: 10 });
const telemetryBatchFlushMs = readIntEnv("TELEMETRY_SQS_BATCH_FLUSH_MS", 5, { min: 0, max: 1000 });
const telemetryBatchMaxBuffer = readIntEnv("TELEMETRY_SQS_BATCH_MAX_BUFFER", 20000, { min: 100, max: 200000 });
const pendingTelemetryMessages: PendingTelemetryMessage[] = [];
let telemetryFlushTimer: ReturnType<typeof setTimeout> | null = null;
let telemetryFlushInProgress = false;
let telemetryEntrySequence = 0;

function nextTelemetryEntryId() {
  telemetryEntrySequence = (telemetryEntrySequence + 1) % 1_000_000_000;
  return `t${Date.now().toString(36)}${telemetryEntrySequence.toString(36)}`;
}

function scheduleTelemetryFlush() {
  if (telemetryFlushInProgress || telemetryFlushTimer !== null) {
    return;
  }

  if (telemetryBatchFlushMs === 0) {
    void flushTelemetryQueue();
    return;
  }

  telemetryFlushTimer = setTimeout(() => {
    telemetryFlushTimer = null;
    void flushTelemetryQueue();
  }, telemetryBatchFlushMs);
}

function resolveBatchResult(batch: PendingTelemetryMessage[], response: TelemetryBatchResponse) {
  const successfulById = new Map<string, { queueMessageId: string | null }>();
  for (const entry of response.Successful ?? []) {
    if (typeof entry.Id === "string") {
      successfulById.set(entry.Id, { queueMessageId: entry.MessageId ?? null });
    }
  }

  const failedById = new Map<string, TelemetryBatchFailedEntry>();
  for (const entry of response.Failed ?? []) {
    if (typeof entry.Id === "string") {
      failedById.set(entry.Id, entry);
    }
  }

  for (const entry of batch) {
    const successful = successfulById.get(entry.entryId);
    if (successful) {
      entry.resolve(successful);
      continue;
    }

    const failed = failedById.get(entry.entryId);
    if (failed) {
      const code = failed.Code ?? "Unknown";
      const message = failed.Message ?? "SQS batch entry failed";
      entry.reject(new Error(`Failed to enqueue telemetry (${code}): ${message}`));
      continue;
    }

    entry.reject(new Error("Failed to enqueue telemetry: missing SQS batch result"));
  }
}

function rejectBatch(batch: PendingTelemetryMessage[], reason: unknown) {
  for (const entry of batch) {
    entry.reject(reason);
  }
}

async function flushTelemetryQueue() {
  if (telemetryFlushInProgress) {
    return;
  }

  if (telemetryFlushTimer !== null) {
    clearTimeout(telemetryFlushTimer);
    telemetryFlushTimer = null;
  }

  telemetryFlushInProgress = true;

  try {
    while (pendingTelemetryMessages.length > 0) {
      const batch = pendingTelemetryMessages.splice(0, telemetryBatchSize);

      try {
        const response = (await sqsClient.send(
          new SendMessageBatchCommand({
            QueueUrl: telemetryQueueUrl,
            Entries: batch.map((entry) => ({
              Id: entry.entryId,
              MessageBody: JSON.stringify(entry.message)
            }))
          })
        )) as TelemetryBatchResponse;

        resolveBatchResult(batch, response);
      } catch (error) {
        rejectBatch(batch, error);
        const remaining = pendingTelemetryMessages.splice(0, pendingTelemetryMessages.length);
        rejectBatch(remaining, error);
        return;
      }
    }
  } finally {
    telemetryFlushInProgress = false;
  }
}

export function enqueueTelemetryMessage(message: TelemetryQueueMessage) {
  return new Promise<{ queueMessageId: string | null }>((resolve, reject) => {
    if (pendingTelemetryMessages.length >= telemetryBatchMaxBuffer) {
      reject(new Error("Telemetry SQS buffer is full"));
      return;
    }

    pendingTelemetryMessages.push({
      entryId: nextTelemetryEntryId(),
      message,
      resolve,
      reject
    });

    if (pendingTelemetryMessages.length >= telemetryBatchSize) {
      void flushTelemetryQueue();
      return;
    }

    scheduleTelemetryFlush();
  });
}

export async function enqueueTelemetryMessages(messages: TelemetryQueueMessage[]) {
  const results = await Promise.all(messages.map((message) => enqueueTelemetryMessage(message)));
  return {
    queuedCount: results.length
  };
}

export async function enqueueControlCommandMessage(message: ControlCommandQueueMessage) {
  const queueUrl = requiredEnv("CONTROL_QUEUE_URL");
  const isFifoQueue = queueUrl.endsWith(".fifo");

  const result = await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(message),
      MessageGroupId: isFifoQueue ? message.deviceId : undefined,
      MessageDeduplicationId: isFifoQueue ? message.idempotencyKey : undefined
    })
  );

  return {
    queueMessageId: result.MessageId ?? null
  };
}
