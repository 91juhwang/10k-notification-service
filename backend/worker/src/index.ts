import { DeleteMessageCommand, ReceiveMessageCommand, SQSClient, type Message } from "@aws-sdk/client-sqs";
import { createPool } from "@microgrid/db";
import type { components } from "@microgrid/shared";

type TelemetryIngestRequest = components["schemas"]["TelemetryIngestRequest"];
type TelemetryQueueMessage = TelemetryIngestRequest & {
  ingestId: string;
  queuedAt: string;
};

interface WorkerConfig {
  pollWaitSeconds: number;
  batchSize: number;
  visibilityTimeoutSeconds: number;
  telemetryQueueUrl: string;
}

let shuttingDown = false;

function requiredEnv(name: string): string {
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

function getConfig(): WorkerConfig {
  return {
    pollWaitSeconds: readIntEnv("WORKER_POLL_WAIT_SECONDS", 20, { min: 0, max: 20 }),
    batchSize: readIntEnv("WORKER_BATCH_SIZE", 10, { min: 1, max: 10 }),
    visibilityTimeoutSeconds: readIntEnv("WORKER_VISIBILITY_TIMEOUT_SECONDS", 45, { min: 0, max: 43200 }),
    telemetryQueueUrl: requiredEnv("TELEMETRY_QUEUE_URL")
  };
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

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isTelemetryQueueMessage(value: unknown): value is TelemetryQueueMessage {
  if (!isObject(value)) {
    return false;
  }

  return (
    typeof value.ingestId === "string" &&
    value.ingestId.length > 0 &&
    typeof value.queuedAt === "string" &&
    !Number.isNaN(Date.parse(value.queuedAt)) &&
    typeof value.deviceId === "string" &&
    value.deviceId.trim().length > 0 &&
    typeof value.timestamp === "string" &&
    !Number.isNaN(Date.parse(value.timestamp)) &&
    typeof value.voltage === "number" &&
    Number.isFinite(value.voltage) &&
    typeof value.frequency === "number" &&
    Number.isFinite(value.frequency) &&
    typeof value.powerKw === "number" &&
    Number.isFinite(value.powerKw)
  );
}

function parseMessageBody(message: Message): { ok: true; value: TelemetryQueueMessage } | { ok: false; error: string } {
  if (!message.Body) {
    return { ok: false, error: "missing message body" };
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(message.Body);
  } catch {
    return { ok: false, error: "invalid JSON body" };
  }

  if (!isTelemetryQueueMessage(parsed)) {
    return { ok: false, error: "invalid telemetry payload shape" };
  }

  return { ok: true, value: parsed };
}

async function deleteMessage(sqsClient: SQSClient, queueUrl: string, message: Message) {
  if (!message.ReceiptHandle) {
    return;
  }

  await sqsClient.send(
    new DeleteMessageCommand({
      QueueUrl: queueUrl,
      ReceiptHandle: message.ReceiptHandle
    })
  );
}

async function persistTelemetry(pool: ReturnType<typeof createPool>, payload: TelemetryQueueMessage) {
  const result = await pool.query<{ id: number }>(
    `
      WITH upsert_device AS (
        INSERT INTO devices (external_device_id)
        VALUES ($1)
        ON CONFLICT (external_device_id)
        DO UPDATE SET updated_at = now()
        RETURNING id
      )
      INSERT INTO telemetry_readings (device_id, ts, voltage, frequency, power_kw, ingest_id)
      SELECT id, $2::timestamptz, $3::double precision, $4::double precision, $5::double precision, $6
      FROM upsert_device
      ON CONFLICT (ingest_id) DO NOTHING
      RETURNING id
    `,
    [
      payload.deviceId,
      payload.timestamp,
      payload.voltage,
      payload.frequency,
      payload.powerKw,
      payload.ingestId
    ]
  );

  return {
    inserted: (result.rowCount ?? 0) > 0
  };
}

async function processMessage(
  sqsClient: SQSClient,
  pool: ReturnType<typeof createPool>,
  queueUrl: string,
  message: Message
) {
  const parsed = parseMessageBody(message);
  if (!parsed.ok) {
    console.warn("[worker] dropping malformed telemetry message", {
      messageId: message.MessageId ?? null,
      reason: parsed.error
    });
    await deleteMessage(sqsClient, queueUrl, message);
    return;
  }

  const persistResult = await persistTelemetry(pool, parsed.value);
  if (!persistResult.inserted) {
    console.info("[worker] duplicate telemetry message ignored", {
      ingestId: parsed.value.ingestId
    });
  }

  await deleteMessage(sqsClient, queueUrl, message);
}

async function pollLoop() {
  const config = getConfig();
  const sqsClient = createSqsClient();
  const pool = createPool();

  console.log("[worker] telemetry worker started", {
    pollWaitSeconds: config.pollWaitSeconds,
    batchSize: config.batchSize,
    visibilityTimeoutSeconds: config.visibilityTimeoutSeconds
  });

  const heartbeat = setInterval(() => {
    console.log("[worker] heartbeat");
  }, 30000);

  process.on("SIGINT", () => {
    shuttingDown = true;
  });
  process.on("SIGTERM", () => {
    shuttingDown = true;
  });

  try {
    while (!shuttingDown) {
      const response = await sqsClient.send(
        new ReceiveMessageCommand({
          QueueUrl: config.telemetryQueueUrl,
          MaxNumberOfMessages: config.batchSize,
          WaitTimeSeconds: config.pollWaitSeconds,
          VisibilityTimeout: config.visibilityTimeoutSeconds
        })
      );

      const messages = response.Messages ?? [];
      if (messages.length === 0) {
        continue;
      }

      for (const message of messages) {
        try {
          await processMessage(sqsClient, pool, config.telemetryQueueUrl, message);
        } catch (error) {
          console.error("[worker] failed to process telemetry message", {
            messageId: message.MessageId ?? null,
            error
          });
        }
      }
    }
  } finally {
    clearInterval(heartbeat);
    await pool.end();
    console.log("[worker] shutdown complete");
  }
}

pollLoop().catch((error) => {
  console.error("[worker] fatal error", error);
  process.exitCode = 1;
});
