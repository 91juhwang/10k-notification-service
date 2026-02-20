import { DeleteMessageCommand, ReceiveMessageCommand, SQSClient, type Message } from "@aws-sdk/client-sqs";
import { createPool } from "@microgrid/db";
import type { components } from "@microgrid/shared";
import { createClient } from "redis";

type TelemetryIngestRequest = components["schemas"]["TelemetryIngestRequest"];
type NotificationRecord = components["schemas"]["Notification"];
type StreamEvent = components["schemas"]["StreamEvent"];
type TelemetryQueueMessage = TelemetryIngestRequest & {
  ingestId: string;
  queuedAt: string;
};
type WorkerPool = ReturnType<typeof createPool>;
type RedisPublisher = ReturnType<typeof createClient>;

interface WorkerConfig {
  pollWaitSeconds: number;
  batchSize: number;
  visibilityTimeoutSeconds: number;
  telemetryQueueUrl: string;
  redisUrl: string;
  redisChannel: string;
}

interface NotificationRow {
  id: number;
  alert_id: number | null;
  type: string;
  message: string;
  is_read: boolean;
  payload: unknown;
  created_at: Date | string;
}

interface PersistTelemetryResult {
  inserted: boolean;
  notifications: NotificationRecord[];
}

interface QueryResult<T> {
  rows: T[];
  rowCount: number | null;
}

interface WorkerDbClient {
  query<T>(queryText: string, values?: unknown[]): Promise<QueryResult<T>>;
  release(): void;
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
    telemetryQueueUrl: requiredEnv("TELEMETRY_QUEUE_URL"),
    redisUrl: requiredEnv("REDIS_URL"),
    redisChannel: process.env.REDIS_CHANNEL ?? "microgrid.events"
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

function createRedisPublisher(redisUrl: string): RedisPublisher {
  const client = createClient({ url: redisUrl });
  client.on("error", (error: unknown) => {
    console.error("[worker] redis publisher error", error);
  });
  return client;
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

function toIsoTimestamp(value: Date | string) {
  if (value instanceof Date) {
    return value.toISOString();
  }

  return new Date(value).toISOString();
}

function toNotificationRecord(row: NotificationRow): NotificationRecord {
  return {
    id: String(row.id),
    alertId: row.alert_id === null ? null : String(row.alert_id),
    type: row.type,
    message: row.message,
    isRead: row.is_read,
    payload: isObject(row.payload) ? row.payload : {},
    createdAt: toIsoTimestamp(row.created_at)
  };
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

async function loadNotificationsByIngestId(client: WorkerDbClient, ingestId: string): Promise<NotificationRecord[]> {
  const result = await client.query<NotificationRow>(
    `
      SELECT id, alert_id, type, message, is_read, payload, created_at
      FROM notifications
      WHERE source_ingest_id = $1
      ORDER BY id ASC
    `,
    [ingestId]
  );

  return result.rows.map(toNotificationRecord);
}

async function persistTelemetry(pool: WorkerPool, payload: TelemetryQueueMessage): Promise<PersistTelemetryResult> {
  const client = (await pool.connect()) as WorkerDbClient;

  try {
    await client.query("BEGIN");

    const telemetryInsert = await client.query<{
      telemetry_reading_id: number;
      device_id: number;
    }>(
      `
        WITH upsert_device AS (
          INSERT INTO devices (external_device_id)
          VALUES ($1)
          ON CONFLICT (external_device_id)
          DO UPDATE SET updated_at = now()
          RETURNING id
        ),
        inserted_telemetry AS (
          INSERT INTO telemetry_readings (device_id, ts, voltage, frequency, power_kw, ingest_id)
          SELECT id, $2::timestamptz, $3::double precision, $4::double precision, $5::double precision, $6
          FROM upsert_device
          ON CONFLICT (ingest_id) DO NOTHING
          RETURNING id, device_id
        )
        SELECT id AS telemetry_reading_id, device_id
        FROM inserted_telemetry
      `,
      [payload.deviceId, payload.timestamp, payload.voltage, payload.frequency, payload.powerKw, payload.ingestId]
    );

    if ((telemetryInsert.rowCount ?? 0) === 0) {
      const notifications = await loadNotificationsByIngestId(client, payload.ingestId);
      await client.query("COMMIT");
      return {
        inserted: false,
        notifications
      };
    }

    const telemetryRow = telemetryInsert.rows[0];

    const alertInsert = await client.query<{ id: number }>(
      `
        INSERT INTO alerts (device_id, telemetry_reading_id, rule_id, severity, message, status)
        SELECT
          $1::bigint,
          $2::bigint,
          r.id,
          r.severity,
          'Rule "' || r.name || '" triggered: ' || r.metric || ' ' || r.operator || ' ' || r.threshold::text ||
            ' (value=' || metric_value.value::text || ')',
          'OPEN'
        FROM alert_rules r
        CROSS JOIN LATERAL (
          SELECT CASE r.metric
            WHEN 'voltage' THEN $3::double precision
            WHEN 'frequency' THEN $4::double precision
            WHEN 'power_kw' THEN $5::double precision
          END AS value
        ) metric_value
        WHERE r.enabled = true
          AND (
            (r.operator = 'gt' AND metric_value.value > r.threshold) OR
            (r.operator = 'gte' AND metric_value.value >= r.threshold) OR
            (r.operator = 'lt' AND metric_value.value < r.threshold) OR
            (r.operator = 'lte' AND metric_value.value <= r.threshold)
          )
        ON CONFLICT (rule_id, telemetry_reading_id) DO NOTHING
        RETURNING id
      `,
      [telemetryRow.device_id, telemetryRow.telemetry_reading_id, payload.voltage, payload.frequency, payload.powerKw]
    );

    let notifications: NotificationRecord[] = [];
    if (alertInsert.rows.length > 0) {
      const alertIds = alertInsert.rows.map((row) => row.id);
      const notificationInsert = await client.query<NotificationRow>(
        `
          INSERT INTO notifications (alert_id, source_ingest_id, type, message, is_read, payload)
          SELECT
            a.id,
            $1,
            'threshold.alert',
            a.message,
            false,
            jsonb_build_object(
              'alertId', a.id,
              'severity', a.severity,
              'deviceId', $2,
              'telemetryReadingId', a.telemetry_reading_id,
              'ingestId', $1
            )
          FROM alerts a
          WHERE a.id = ANY($3::bigint[])
          RETURNING id, alert_id, type, message, is_read, payload, created_at
        `,
        [payload.ingestId, payload.deviceId, alertIds]
      );

      notifications = notificationInsert.rows.map(toNotificationRecord);
    }

    await client.query("COMMIT");
    return {
      inserted: true,
      notifications
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function publishNotificationEvents(
  redisPublisher: RedisPublisher,
  redisChannel: string,
  notifications: NotificationRecord[]
) {
  for (const notification of notifications) {
    const event: StreamEvent = {
      eventType: "notification.created",
      timestamp: new Date().toISOString(),
      data: notification
    };

    await redisPublisher.publish(redisChannel, JSON.stringify(event));
  }
}

async function processMessage(
  sqsClient: SQSClient,
  redisPublisher: RedisPublisher,
  pool: WorkerPool,
  queueUrl: string,
  redisChannel: string,
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

  if (persistResult.notifications.length > 0) {
    await publishNotificationEvents(redisPublisher, redisChannel, persistResult.notifications);
  }

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
  const redisPublisher = createRedisPublisher(config.redisUrl);
  const pool = createPool();

  await redisPublisher.connect();

  console.log("[worker] telemetry worker started", {
    pollWaitSeconds: config.pollWaitSeconds,
    batchSize: config.batchSize,
    visibilityTimeoutSeconds: config.visibilityTimeoutSeconds,
    redisChannel: config.redisChannel
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
          await processMessage(
            sqsClient,
            redisPublisher,
            pool,
            config.telemetryQueueUrl,
            config.redisChannel,
            message
          );
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
    if (redisPublisher.isOpen) {
      await redisPublisher.quit();
    }
    await pool.end();
    console.log("[worker] shutdown complete");
  }
}

pollLoop().catch((error) => {
  console.error("[worker] fatal error", error);
  process.exitCode = 1;
});
