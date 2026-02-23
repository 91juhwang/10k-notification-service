import {
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SQSClient,
  type Message
} from "@aws-sdk/client-sqs";
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
  queueBootstrapTimeoutSeconds: number;
  queueBootstrapRetryMs: number;
  telemetryQueueUrl: string;
  controlQueueUrl: string;
  telemetryDlqQueueUrl: string | null;
  controlDlqQueueUrl: string | null;
  metricsLogIntervalSeconds: number;
  redisUrl: string;
  redisChannel: string;
}

interface QueueRef {
  name: string;
  url: string;
}

interface QueueDepthSnapshot {
  visible: number;
  inFlight: number;
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

interface WorkerMetrics {
  startedAtMs: number;
  telemetryProcessed: number;
  telemetryDuplicate: number;
  telemetryMalformed: number;
  telemetryFailed: number;
  controlProcessed: number;
  controlMalformed: number;
  controlFailed: number;
  controlMissing: number;
  controlTerminal: number;
  telemetryLatencySamplesMs: number[];
  controlLatencySamplesMs: number[];
}

const maxLatencySamples = 2000;
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

function optionalEnv(name: string): string | null {
  const value = process.env[name];
  if (!value) {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function getConfig(): WorkerConfig {
  return {
    pollWaitSeconds: readIntEnv("WORKER_POLL_WAIT_SECONDS", 20, { min: 0, max: 20 }),
    batchSize: readIntEnv("WORKER_BATCH_SIZE", 10, { min: 1, max: 10 }),
    visibilityTimeoutSeconds: readIntEnv("WORKER_VISIBILITY_TIMEOUT_SECONDS", 45, { min: 0, max: 43200 }),
    queueBootstrapTimeoutSeconds: readIntEnv("WORKER_QUEUE_BOOTSTRAP_TIMEOUT_SECONDS", 120, { min: 1, max: 3600 }),
    queueBootstrapRetryMs: readIntEnv("WORKER_QUEUE_BOOTSTRAP_RETRY_MS", 1000, { min: 100, max: 30000 }),
    telemetryQueueUrl: requiredEnv("TELEMETRY_QUEUE_URL"),
    controlQueueUrl: requiredEnv("CONTROL_QUEUE_URL"),
    telemetryDlqQueueUrl: optionalEnv("TELEMETRY_DLQ_URL"),
    controlDlqQueueUrl: optionalEnv("CONTROL_DLQ_URL"),
    metricsLogIntervalSeconds: readIntEnv("WORKER_METRICS_LOG_INTERVAL_SECONDS", 30, { min: 5, max: 3600 }),
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

function createMetrics(): WorkerMetrics {
  return {
    startedAtMs: Date.now(),
    telemetryProcessed: 0,
    telemetryDuplicate: 0,
    telemetryMalformed: 0,
    telemetryFailed: 0,
    controlProcessed: 0,
    controlMalformed: 0,
    controlFailed: 0,
    controlMissing: 0,
    controlTerminal: 0,
    telemetryLatencySamplesMs: [],
    controlLatencySamplesMs: []
  };
}

function toInt(raw: string | undefined) {
  if (!raw) {
    return 0;
  }

  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function pushLatencySample(samples: number[], value: number) {
  if (!Number.isFinite(value) || value < 0) {
    return;
  }

  if (samples.length >= maxLatencySamples) {
    samples.shift();
  }

  samples.push(value);
}

function recordEndToEndLatency(samples: number[], originIsoTimestamp: string) {
  const originMs = Date.parse(originIsoTimestamp);
  if (Number.isNaN(originMs)) {
    return;
  }

  pushLatencySample(samples, Date.now() - originMs);
}

function percentile(samples: number[], ratio: number) {
  if (samples.length === 0) {
    return null;
  }

  const sorted = [...samples].sort((left, right) => left - right);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
  return sorted[index];
}

function summarizeLatency(samples: number[]) {
  if (samples.length === 0) {
    return null;
  }

  const sum = samples.reduce((accumulator, value) => accumulator + value, 0);
  const p50Ms = percentile(samples, 0.5);
  const p95Ms = percentile(samples, 0.95);

  return {
    sampleCount: samples.length,
    avgMs: Number((sum / samples.length).toFixed(2)),
    p50Ms: p50Ms === null ? null : Number(p50Ms.toFixed(2)),
    p95Ms: p95Ms === null ? null : Number(p95Ms.toFixed(2))
  };
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

function toErrorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === "string") {
    return error;
  }

  return "unknown error";
}

function isQueueDoesNotExistError(error: unknown) {
  const message = toErrorMessage(error);
  if (/QueueDoesNotExist|does not exist/i.test(message)) {
    return true;
  }

  if (typeof error !== "object" || error === null) {
    return false;
  }

  const value = error as {
    name?: unknown;
    Code?: unknown;
    __type?: unknown;
    Error?: { Code?: unknown; Message?: unknown };
  };

  const fields = [value.name, value.Code, value.__type, value.Error?.Code, value.Error?.Message];
  return fields.some((field) => typeof field === "string" && /QueueDoesNotExist|does not exist/i.test(field));
}

async function waitForQueueAvailability(
  sqsClient: SQSClient,
  queueRef: QueueRef,
  { timeoutMs, retryMs }: { timeoutMs: number; retryMs: number }
) {
  const deadline = Date.now() + timeoutMs;
  let attempts = 0;

  while (!shuttingDown) {
    try {
      await getQueueDepth(sqsClient, queueRef.url);
      if (attempts > 0) {
        console.info("[worker] queue became available", {
          queueName: queueRef.name,
          queueUrl: queueRef.url,
          attempts
        });
      }
      return;
    } catch (error) {
      attempts += 1;

      if (Date.now() >= deadline) {
        throw new Error(
          `Queue "${queueRef.name}" not available after ${Math.round(timeoutMs / 1000)}s: ${toErrorMessage(error)}`
        );
      }

      if (attempts === 1 || attempts % 10 === 0) {
        console.info("[worker] waiting for queue availability", {
          queueName: queueRef.name,
          queueUrl: queueRef.url,
          reason: isQueueDoesNotExistError(error) ? "QueueDoesNotExist" : toErrorMessage(error),
          attempts,
          retryMs
        });
      }

      await sleep(retryMs);
    }
  }
}

async function getQueueDepth(sqsClient: SQSClient, queueUrl: string): Promise<QueueDepthSnapshot> {
  const result = await sqsClient.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    })
  );

  return {
    visible: toInt(result.Attributes?.ApproximateNumberOfMessages),
    inFlight: toInt(result.Attributes?.ApproximateNumberOfMessagesNotVisible)
  };
}

async function getQueueDepths(sqsClient: SQSClient, queueRefs: QueueRef[]) {
  const snapshots = await Promise.all(
    queueRefs.map(async (queueRef) => {
      try {
        const depth = await getQueueDepth(sqsClient, queueRef.url);
        return {
          name: queueRef.name,
          snapshot: depth
        };
      } catch (error) {
        return {
          name: queueRef.name,
          snapshot: {
            error: error instanceof Error ? error.message : "unknown queue depth error"
          }
        };
      }
    })
  );

  return Object.fromEntries(snapshots.map((entry) => [entry.name, entry.snapshot]));
}

async function logMetricsSnapshot(sqsClient: SQSClient, queueRefs: QueueRef[], metrics: WorkerMetrics) {
  const queueDepth = await getQueueDepths(sqsClient, queueRefs);

  console.log("[worker] metrics", {
    uptimeSeconds: Math.floor((Date.now() - metrics.startedAtMs) / 1000),
    telemetry: {
      processed: metrics.telemetryProcessed,
      duplicate: metrics.telemetryDuplicate,
      malformed: metrics.telemetryMalformed,
      failed: metrics.telemetryFailed,
      endToEndLatency: summarizeLatency(metrics.telemetryLatencySamplesMs)
    },
    control: {
      processed: metrics.controlProcessed,
      malformed: metrics.controlMalformed,
      failed: metrics.controlFailed,
      missing: metrics.controlMissing,
      terminal: metrics.controlTerminal,
      endToEndLatency: summarizeLatency(metrics.controlLatencySamplesMs)
    },
    queues: queueDepth
  });
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
          VALUES ($1::text)
          ON CONFLICT (external_device_id)
          DO UPDATE SET updated_at = now()
          RETURNING id
        ),
        inserted_telemetry AS (
          INSERT INTO telemetry_readings (device_id, ts, voltage, frequency, power_kw, ingest_id)
          SELECT id, $2::timestamptz, $3::double precision, $4::double precision, $5::double precision, $6::text
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
            $1::text,
            'threshold.alert',
            a.message,
            false,
            jsonb_build_object(
              'alertId', a.id,
              'severity', a.severity,
              'deviceId', $2::text,
              'telemetryReadingId', a.telemetry_reading_id,
              'ingestId', $1::text
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
  message: Message,
  metrics: WorkerMetrics
) {
  const parsed = parseTelemetryMessageBody(message);
  if (!parsed.ok) {
    metrics.telemetryMalformed += 1;
    console.warn("[worker] malformed telemetry message (will retry until DLQ)", {
      messageId: message.MessageId ?? null,
      receiveCount: message.Attributes?.ApproximateReceiveCount ?? null,
      reason: parsed.error
    });
    return;
  }

  const persistResult = await persistTelemetry(pool, parsed.value);
  metrics.telemetryProcessed += 1;
  recordEndToEndLatency(metrics.telemetryLatencySamplesMs, parsed.value.queuedAt);

  if (persistResult.notifications.length > 0) {
    await publishNotificationEvents(redisPublisher, redisChannel, persistResult.notifications);
  }

  if (!persistResult.inserted) {
    metrics.telemetryDuplicate += 1;
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
  message: Message,
  metrics: WorkerMetrics
) {
  const parsed = parseControlMessageBody(message);
  if (!parsed.ok) {
    metrics.controlMalformed += 1;
    console.warn("[worker] malformed control message (will retry until DLQ)", {
      messageId: message.MessageId ?? null,
      receiveCount: message.Attributes?.ApproximateReceiveCount ?? null,
      reason: parsed.error
    });
    return;
  }

  const lifecycle = await runControlCommandLifecycle(pool, parsed.value);

  if (lifecycle.kind === "missing") {
    metrics.controlMissing += 1;
    console.warn("[worker] control command not found for queue message", {
      commandId: parsed.value.commandId
    });
    await deleteMessage(sqsClient, queueUrl, message);
    return;
  }

  if (lifecycle.kind === "terminal") {
    metrics.controlTerminal += 1;
    console.info("[worker] control command already in terminal state", {
      commandId: parsed.value.commandId
    });
    await deleteMessage(sqsClient, queueUrl, message);
    return;
  }

  metrics.controlProcessed += 1;
  recordEndToEndLatency(metrics.controlLatencySamplesMs, parsed.value.createdAt);
  await publishControlEvents(redisPublisher, redisChannel, lifecycle.events);
  await deleteMessage(sqsClient, queueUrl, message);
}

async function pollLoop() {
  const config = getConfig();
  const sqsClient = createSqsClient();
  const redisPublisher = createRedisPublisher(config.redisUrl);
  const pool = createPool();
  const metrics = createMetrics();
  const requiredQueueRefs: QueueRef[] = [
    { name: "telemetry", url: config.telemetryQueueUrl },
    { name: "control", url: config.controlQueueUrl }
  ];
  const queueRefs: QueueRef[] = [...requiredQueueRefs];
  if (config.telemetryDlqQueueUrl) {
    queueRefs.push({ name: "telemetryDlq", url: config.telemetryDlqQueueUrl });
  }
  if (config.controlDlqQueueUrl) {
    queueRefs.push({ name: "controlDlq", url: config.controlDlqQueueUrl });
  }

  await redisPublisher.connect();

  console.log("[worker] worker started", {
    pollWaitSeconds: config.pollWaitSeconds,
    batchSize: config.batchSize,
    visibilityTimeoutSeconds: config.visibilityTimeoutSeconds,
    telemetryQueueUrl: config.telemetryQueueUrl,
    controlQueueUrl: config.controlQueueUrl,
    telemetryDlqQueueUrl: config.telemetryDlqQueueUrl,
    controlDlqQueueUrl: config.controlDlqQueueUrl,
    metricsLogIntervalSeconds: config.metricsLogIntervalSeconds,
    redisChannel: config.redisChannel
  });

  const heartbeat = setInterval(() => {
    void logMetricsSnapshot(sqsClient, queueRefs, metrics).catch((error) => {
      console.error("[worker] failed to capture metrics", error);
    });
  }, config.metricsLogIntervalSeconds * 1000);

  process.on("SIGINT", () => {
    shuttingDown = true;
  });
  process.on("SIGTERM", () => {
    shuttingDown = true;
  });

  const queueBootstrapTimeoutMs = config.queueBootstrapTimeoutSeconds * 1000;
  for (const queueRef of requiredQueueRefs) {
    await waitForQueueAvailability(sqsClient, queueRef, {
      timeoutMs: queueBootstrapTimeoutMs,
      retryMs: config.queueBootstrapRetryMs
    });
  }

  try {
    while (!shuttingDown) {
      const [telemetryResult, controlResult] = await Promise.allSettled([
        sqsClient.send(
          new ReceiveMessageCommand({
            QueueUrl: config.telemetryQueueUrl,
            MaxNumberOfMessages: config.batchSize,
            WaitTimeSeconds: config.pollWaitSeconds,
            VisibilityTimeout: config.visibilityTimeoutSeconds,
            MessageSystemAttributeNames: ["ApproximateReceiveCount"]
          })
        ),
        sqsClient.send(
          new ReceiveMessageCommand({
            QueueUrl: config.controlQueueUrl,
            MaxNumberOfMessages: config.batchSize,
            WaitTimeSeconds: config.pollWaitSeconds,
            VisibilityTimeout: config.visibilityTimeoutSeconds,
            MessageSystemAttributeNames: ["ApproximateReceiveCount"]
          })
        )
      ]);

      if (telemetryResult.status === "rejected") {
        console.warn("[worker] failed to poll telemetry queue", {
          queueUrl: config.telemetryQueueUrl,
          error: telemetryResult.reason
        });
      }

      if (controlResult.status === "rejected") {
        console.warn("[worker] failed to poll control queue", {
          queueUrl: config.controlQueueUrl,
          error: controlResult.reason
        });
      }

      const telemetryMessages = telemetryResult.status === "fulfilled" ? telemetryResult.value.Messages ?? [] : [];
      const controlMessages = controlResult.status === "fulfilled" ? controlResult.value.Messages ?? [] : [];

      if (telemetryMessages.length === 0 && controlMessages.length === 0) {
        if (telemetryResult.status === "rejected" && controlResult.status === "rejected") {
          await sleep(1000);
        }
        continue;
      }

      await Promise.all([
        Promise.all(
          telemetryMessages.map(async (message) => {
            try {
              await processTelemetryMessage(
                sqsClient,
                redisPublisher,
                pool,
                config.telemetryQueueUrl,
                config.redisChannel,
                message,
                metrics
              );
            } catch (error) {
              metrics.telemetryFailed += 1;
              console.error("[worker] failed to process telemetry message", {
                messageId: message.MessageId ?? null,
                error
              });
            }
          })
        ),
        Promise.all(
          controlMessages.map(async (message) => {
            try {
              await processControlMessage(
                sqsClient,
                redisPublisher,
                pool,
                config.controlQueueUrl,
                config.redisChannel,
                message,
                metrics
              );
            } catch (error) {
              metrics.controlFailed += 1;
              console.error("[worker] failed to process control message", {
                messageId: message.MessageId ?? null,
                error
              });
            }
          })
        )
      ]);
    }
  } finally {
    clearInterval(heartbeat);
    await logMetricsSnapshot(sqsClient, queueRefs, metrics).catch((error) => {
      console.error("[worker] failed to capture final metrics", error);
    });
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
