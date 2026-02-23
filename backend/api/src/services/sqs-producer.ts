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

interface TelemetryBufferWaiter {
  id: number;
  resolve: (hasCapacity: boolean) => void;
  timeout: ReturnType<typeof setTimeout> | null;
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

interface TelemetryBatchFailure {
  entry: PendingTelemetryMessage;
  code: string;
  message: string;
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
const telemetryBatchFlushConcurrency = readIntEnv("TELEMETRY_SQS_FLUSH_CONCURRENCY", 6, { min: 1, max: 64 });
const telemetryBatchRetryAttempts = readIntEnv("TELEMETRY_SQS_BATCH_RETRY_ATTEMPTS", 3, { min: 0, max: 10 });
const telemetryBatchRetryBaseMs = readIntEnv("TELEMETRY_SQS_BATCH_RETRY_BASE_MS", 25, { min: 1, max: 5000 });
const telemetryBufferWaitMs = readIntEnv("TELEMETRY_SQS_BUFFER_WAIT_MS", 200, { min: 0, max: 60000 });
const pendingTelemetryMessages: PendingTelemetryMessage[] = [];
const telemetryBufferWaiters: TelemetryBufferWaiter[] = [];
let telemetryFlushTimer: ReturnType<typeof setTimeout> | null = null;
let telemetryFlushWorkersInProgress = 0;
let telemetryEntrySequence = 0;
let telemetryBufferWaiterSequence = 0;

function nextTelemetryEntryId() {
  telemetryEntrySequence = (telemetryEntrySequence + 1) % 1_000_000_000;
  return `t${Date.now().toString(36)}${telemetryEntrySequence.toString(36)}`;
}

function nextTelemetryBufferWaiterId() {
  telemetryBufferWaiterSequence = (telemetryBufferWaiterSequence + 1) % 1_000_000_000;
  return telemetryBufferWaiterSequence;
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

function computeRetryBackoffMs(attempt: number) {
  const exponent = Math.max(0, attempt - 1);
  const ms = telemetryBatchRetryBaseMs * 2 ** exponent;
  return Math.min(ms, 5000);
}

function notifyTelemetryBufferCapacity() {
  while (telemetryBufferWaiters.length > 0 && pendingTelemetryMessages.length < telemetryBatchMaxBuffer) {
    const waiter = telemetryBufferWaiters.shift();
    if (!waiter) {
      break;
    }

    if (waiter.timeout !== null) {
      clearTimeout(waiter.timeout);
    }
    waiter.resolve(true);
  }
}

function waitForTelemetryBufferCapacity() {
  if (pendingTelemetryMessages.length < telemetryBatchMaxBuffer) {
    return Promise.resolve(true);
  }

  if (telemetryBufferWaitMs === 0) {
    return Promise.resolve(false);
  }

  return new Promise<boolean>((resolve) => {
    const waiterId = nextTelemetryBufferWaiterId();
    const waiter: TelemetryBufferWaiter = {
      id: waiterId,
      resolve,
      timeout: null
    };

    waiter.timeout = setTimeout(() => {
      const index = telemetryBufferWaiters.findIndex((current) => current.id === waiterId);
      if (index >= 0) {
        telemetryBufferWaiters.splice(index, 1);
      }
      resolve(false);
    }, telemetryBufferWaitMs);

    telemetryBufferWaiters.push(waiter);
  });
}

function takeTelemetryBatch() {
  if (pendingTelemetryMessages.length === 0) {
    return [] as PendingTelemetryMessage[];
  }

  const batch = pendingTelemetryMessages.splice(0, telemetryBatchSize);
  notifyTelemetryBufferCapacity();
  return batch;
}

function scheduleTelemetryFlush() {
  if (telemetryFlushTimer !== null) {
    return;
  }

  if (telemetryBatchFlushMs === 0) {
    requestTelemetryFlush();
    return;
  }

  telemetryFlushTimer = setTimeout(() => {
    telemetryFlushTimer = null;
    requestTelemetryFlush();
  }, telemetryBatchFlushMs);
}

function resolveTelemetryBatchAndCollectFailures(batch: PendingTelemetryMessage[], response: TelemetryBatchResponse) {
  const byId = new Map<string, PendingTelemetryMessage>(batch.map((entry) => [entry.entryId, entry]));
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

  const failures: TelemetryBatchFailure[] = [];
  for (const [entryId, telemetryEntry] of byId.entries()) {
    const successful = successfulById.get(entryId);
    if (successful) {
      telemetryEntry.resolve(successful);
      continue;
    }

    const failed = failedById.get(entryId);
    if (failed) {
      const code = failed.Code ?? "Unknown";
      const message = failed.Message ?? "SQS batch entry failed";
      failures.push({
        entry: telemetryEntry,
        code,
        message
      });
      continue;
    }

    failures.push({
      entry: telemetryEntry,
      code: "Unknown",
      message: "missing SQS batch result"
    });
  }

  return failures;
}

function rejectBatch(batch: PendingTelemetryMessage[], reason: unknown) {
  for (const entry of batch) {
    entry.reject(reason);
  }
}

function rejectBatchFailures(failures: TelemetryBatchFailure[], suffix: string) {
  for (const failure of failures) {
    failure.entry.reject(new Error(`Failed to enqueue telemetry (${failure.code}): ${failure.message}${suffix}`));
  }
}

async function sendTelemetryBatchWithRetry(batch: PendingTelemetryMessage[]) {
  let currentBatch = batch;
  let currentFailures: TelemetryBatchFailure[] = [];

  for (let attempt = 1; attempt <= telemetryBatchRetryAttempts + 1; attempt += 1) {
    try {
      const response = (await sqsClient.send(
        new SendMessageBatchCommand({
          QueueUrl: telemetryQueueUrl,
          Entries: currentBatch.map((entry) => ({
            Id: entry.entryId,
            MessageBody: JSON.stringify(entry.message)
          }))
        })
      )) as TelemetryBatchResponse;

      currentFailures = resolveTelemetryBatchAndCollectFailures(currentBatch, response);
      if (currentFailures.length === 0) {
        return;
      }

      if (attempt > telemetryBatchRetryAttempts) {
        rejectBatchFailures(currentFailures, " (retry limit reached)");
        return;
      }

      currentBatch = currentFailures.map((failure) => failure.entry);
      await sleep(computeRetryBackoffMs(attempt));
      continue;
    } catch (error) {
      if (attempt > telemetryBatchRetryAttempts) {
        rejectBatch(currentBatch, error);
        return;
      }

      await sleep(computeRetryBackoffMs(attempt));
    }
  }
}

async function runTelemetryFlushWorker() {
  try {
    while (true) {
      const batch = takeTelemetryBatch();
      if (batch.length === 0) {
        break;
      }

      await sendTelemetryBatchWithRetry(batch);
    }
  } finally {
    telemetryFlushWorkersInProgress -= 1;
    if (pendingTelemetryMessages.length > 0) {
      requestTelemetryFlush();
    }
  }
}

function requestTelemetryFlush() {
  if (telemetryFlushTimer !== null) {
    clearTimeout(telemetryFlushTimer);
    telemetryFlushTimer = null;
  }

  while (
    telemetryFlushWorkersInProgress < telemetryBatchFlushConcurrency &&
    pendingTelemetryMessages.length > 0
  ) {
    telemetryFlushWorkersInProgress += 1;
    void runTelemetryFlushWorker();
  }
}

export async function enqueueTelemetryMessage(message: TelemetryQueueMessage) {
  while (pendingTelemetryMessages.length >= telemetryBatchMaxBuffer) {
    const hasCapacity = await waitForTelemetryBufferCapacity();
    if (!hasCapacity) {
      throw new Error("Telemetry SQS buffer is full");
    }
  }

  return new Promise<{ queueMessageId: string | null }>((resolve, reject) => {
    pendingTelemetryMessages.push({
      entryId: nextTelemetryEntryId(),
      message,
      resolve,
      reject
    });

    if (pendingTelemetryMessages.length >= telemetryBatchSize) {
      requestTelemetryFlush();
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
