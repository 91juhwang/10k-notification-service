import { DeleteMessageCommand, ReceiveMessageCommand, SQSClient, type Message } from "@aws-sdk/client-sqs";
import { createPool } from "@microgrid/db";
import type { components } from "@microgrid/shared";
import { createClient } from "redis";

type TelemetryIngestRequest = components["schemas"]["TelemetryIngestRequest"];
type ControlCommandRequest = components["schemas"]["ControlCommandRequest"];
type NotificationRecord = components["schemas"]["Notification"];
type ControlCommandRecord = components["schemas"]["ControlCommand"];
type ControlCommandStatus = ControlCommandRecord["status"];
type StreamEvent = components["schemas"]["StreamEvent"];
type TelemetryQueueMessage = TelemetryIngestRequest & {
  ingestId: string;
  queuedAt: string;
};
type ControlQueueMessage = ControlCommandRequest & {
  commandId: string;
  requestedBy: string | null;
  createdAt: string;
};
type WorkerPool = ReturnType<typeof createPool>;
type RedisPublisher = ReturnType<typeof createClient>;

interface WorkerConfig {
  pollWaitSeconds: number;
  batchSize: number;
  visibilityTimeoutSeconds: number;
  telemetryQueueUrl: string;
  controlQueueUrl: string;
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

interface ControlCommandRow {
  id: string;
  external_device_id: string;
  command_type: string;
  payload: unknown;
  status: string;
  idempotency_key: string;
  requested_by: string | null;
  error_message: string | null;
  created_at: Date | string;
  updated_at: Date | string;
}

interface PersistTelemetryResult {
  inserted: boolean;
  notifications: NotificationRecord[];
}

interface ControlLifecycleResult {
  kind: "processed" | "missing" | "terminal";
  events: ControlCommandRecord[];
}

interface QueryResult<T> {
  rows: T[];
  rowCount: number | null;
}

interface WorkerDbClient {
  query<T>(queryText: string, values?: unknown[]): Promise<QueryResult<T>>;
  release(): void;
}

const controlStatuses = new Set<ControlCommandStatus>(["queued", "processing", "succeeded", "failed", "cancelled"]);
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
    controlQueueUrl: requiredEnv("CONTROL_QUEUE_URL"),
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

function isControlCommandStatus(value: unknown): value is ControlCommandStatus {
  return typeof value === "string" && controlStatuses.has(value as ControlCommandStatus);
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

function isControlQueueMessage(value: unknown): value is ControlQueueMessage {
  if (!isObject(value)) {
    return false;
  }

  return (
    typeof value.commandId === "string" &&
    value.commandId.length > 0 &&
    typeof value.deviceId === "string" &&
    value.deviceId.trim().length > 0 &&
    typeof value.commandType === "string" &&
    value.commandType.trim().length > 0 &&
    isObject(value.payload) &&
    typeof value.idempotencyKey === "string" &&
    value.idempotencyKey.trim().length > 0 &&
    typeof value.createdAt === "string" &&
    !Number.isNaN(Date.parse(value.createdAt)) &&
    (value.requestedBy === null || typeof value.requestedBy === "string")
  );
}

function parseTelemetryMessageBody(
  message: Message
): { ok: true; value: TelemetryQueueMessage } | { ok: false; error: string } {
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

function parseControlMessageBody(
  message: Message
): { ok: true; value: ControlQueueMessage } | { ok: false; error: string } {
  if (!message.Body) {
    return { ok: false, error: "missing message body" };
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(message.Body);
  } catch {
    return { ok: false, error: "invalid JSON body" };
  }

  if (!isControlQueueMessage(parsed)) {
    return { ok: false, error: "invalid control payload shape" };
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

function toControlCommandRecord(row: ControlCommandRow): ControlCommandRecord {
  return {
    id: row.id,
    deviceId: row.external_device_id,
    commandType: row.command_type,
    payload: isObject(row.payload) ? row.payload : {},
    status: row.status as ControlCommandStatus,
    idempotencyKey: row.idempotency_key,
    requestedBy: row.requested_by,
    createdAt: toIsoTimestamp(row.created_at),
    updatedAt: toIsoTimestamp(row.updated_at)
  };
}

function shouldForceFail(payload: Record<string, unknown>) {
  return payload.forceFail === true;
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

async function runControlCommandLifecycle(
  pool: WorkerPool,
  payload: ControlQueueMessage
): Promise<ControlLifecycleResult> {
  const client = (await pool.connect()) as WorkerDbClient;

  try {
    await client.query("BEGIN");

    const existingResult = await client.query<ControlCommandRow>(
      `
        SELECT
          id,
          external_device_id,
          command_type,
          payload,
          status,
          idempotency_key,
          requested_by,
          error_message,
          created_at,
          updated_at
        FROM control_commands
        WHERE id = $1
        FOR UPDATE
      `,
      [payload.commandId]
    );

    const existingRow = existingResult.rows[0];
    if (!existingRow || !isControlCommandStatus(existingRow.status)) {
      await client.query("COMMIT");
      return {
        kind: "missing",
        events: []
      };
    }

    if (existingRow.status === "succeeded" || existingRow.status === "failed" || existingRow.status === "cancelled") {
      await client.query("COMMIT");
      return {
        kind: "terminal",
        events: []
      };
    }

    const processingResult = await client.query<ControlCommandRow>(
      `
        UPDATE control_commands
        SET status = 'processing',
            error_message = NULL,
            updated_at = now()
        WHERE id = $1
        RETURNING
          id,
          external_device_id,
          command_type,
          payload,
          status,
          idempotency_key,
          requested_by,
          error_message,
          created_at,
          updated_at
      `,
      [payload.commandId]
    );

    const processingRow = processingResult.rows[0];
    if (!processingRow || !isControlCommandStatus(processingRow.status)) {
      await client.query("ROLLBACK");
      throw new Error("Failed to transition control command to processing state");
    }

    const finalStatus: ControlCommandStatus = shouldForceFail(payload.payload) ? "failed" : "succeeded";
    const errorMessage =
      finalStatus === "failed" ? "Simulated command failure (set payload.forceFail=true to trigger)" : null;

    const finalResult = await client.query<ControlCommandRow>(
      `
        UPDATE control_commands
        SET status = $2,
            error_message = $3,
            updated_at = now()
        WHERE id = $1
        RETURNING
          id,
          external_device_id,
          command_type,
          payload,
          status,
          idempotency_key,
          requested_by,
          error_message,
          created_at,
          updated_at
      `,
      [payload.commandId, finalStatus, errorMessage]
    );

    const finalRow = finalResult.rows[0];
    if (!finalRow || !isControlCommandStatus(finalRow.status)) {
      await client.query("ROLLBACK");
      throw new Error("Failed to finalize control command status");
    }

    await client.query("COMMIT");

    return {
      kind: "processed",
      events: [toControlCommandRecord(processingRow), toControlCommandRecord(finalRow)]
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function publishStreamEvent(redisPublisher: RedisPublisher, redisChannel: string, event: StreamEvent) {
  await redisPublisher.publish(redisChannel, JSON.stringify(event));
}

async function publishNotificationEvents(
  redisPublisher: RedisPublisher,
  redisChannel: string,
  notifications: NotificationRecord[]
) {
  for (const notification of notifications) {
    await publishStreamEvent(redisPublisher, redisChannel, {
      eventType: "notification.created",
      timestamp: new Date().toISOString(),
      data: notification
    });
  }
}

async function publishControlEvents(
  redisPublisher: RedisPublisher,
  redisChannel: string,
  controlEvents: ControlCommandRecord[]
) {
  for (const command of controlEvents) {
    await publishStreamEvent(redisPublisher, redisChannel, {
      eventType: "control.updated",
      timestamp: new Date().toISOString(),
      data: {
        command
      }
    });
  }
}

async function processTelemetryMessage(
  sqsClient: SQSClient,
  redisPublisher: RedisPublisher,
  pool: WorkerPool,
  queueUrl: string,
  redisChannel: string,
  message: Message
) {
  const parsed = parseTelemetryMessageBody(message);
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

async function processControlMessage(
  sqsClient: SQSClient,
  redisPublisher: RedisPublisher,
  pool: WorkerPool,
  queueUrl: string,
  redisChannel: string,
  message: Message
) {
  const parsed = parseControlMessageBody(message);
  if (!parsed.ok) {
    console.warn("[worker] dropping malformed control message", {
      messageId: message.MessageId ?? null,
      reason: parsed.error
    });
    await deleteMessage(sqsClient, queueUrl, message);
    return;
  }

  const lifecycle = await runControlCommandLifecycle(pool, parsed.value);

  if (lifecycle.kind === "missing") {
    console.warn("[worker] control command not found for queue message", {
      commandId: parsed.value.commandId
    });
    await deleteMessage(sqsClient, queueUrl, message);
    return;
  }

  if (lifecycle.kind === "terminal") {
    console.info("[worker] control command already in terminal state", {
      commandId: parsed.value.commandId
    });
    await deleteMessage(sqsClient, queueUrl, message);
    return;
  }

  await publishControlEvents(redisPublisher, redisChannel, lifecycle.events);
  await deleteMessage(sqsClient, queueUrl, message);
}

async function pollLoop() {
  const config = getConfig();
  const sqsClient = createSqsClient();
  const redisPublisher = createRedisPublisher(config.redisUrl);
  const pool = createPool();

  await redisPublisher.connect();

  console.log("[worker] worker started", {
    pollWaitSeconds: config.pollWaitSeconds,
    batchSize: config.batchSize,
    visibilityTimeoutSeconds: config.visibilityTimeoutSeconds,
    telemetryQueueUrl: config.telemetryQueueUrl,
    controlQueueUrl: config.controlQueueUrl,
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
      const [telemetryResponse, controlResponse] = await Promise.all([
        sqsClient.send(
          new ReceiveMessageCommand({
            QueueUrl: config.telemetryQueueUrl,
            MaxNumberOfMessages: config.batchSize,
            WaitTimeSeconds: config.pollWaitSeconds,
            VisibilityTimeout: config.visibilityTimeoutSeconds
          })
        ),
        sqsClient.send(
          new ReceiveMessageCommand({
            QueueUrl: config.controlQueueUrl,
            MaxNumberOfMessages: config.batchSize,
            WaitTimeSeconds: config.pollWaitSeconds,
            VisibilityTimeout: config.visibilityTimeoutSeconds
          })
        )
      ]);

      const telemetryMessages = telemetryResponse.Messages ?? [];
      const controlMessages = controlResponse.Messages ?? [];

      for (const message of telemetryMessages) {
        try {
          await processTelemetryMessage(
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

      for (const message of controlMessages) {
        try {
          await processControlMessage(
            sqsClient,
            redisPublisher,
            pool,
            config.controlQueueUrl,
            config.redisChannel,
            message
          );
        } catch (error) {
          console.error("[worker] failed to process control message", {
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
