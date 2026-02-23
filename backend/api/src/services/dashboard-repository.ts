import { createPool } from "@microgrid/db";
import type { components } from "@microgrid/shared";

type AlertRecord = components["schemas"]["Alert"];
type AlertStatus = AlertRecord["status"];
type AlertSeverity = AlertRecord["severity"];
type NotificationRecord = components["schemas"]["Notification"];
type NotificationAckResponse = components["schemas"]["NotificationAckResponse"];

type DbValue = string | number | boolean | null;

interface AlertRow {
  id: string | number;
  external_device_id: string;
  severity: AlertSeverity;
  message: string;
  status: AlertStatus;
  created_at: Date | string;
  updated_at: Date | string;
}

interface NotificationRow {
  id: string | number;
  alert_id: string | number | null;
  type: string;
  message: string;
  is_read: boolean;
  payload: unknown;
  created_at: Date | string;
  updated_at?: Date | string;
}

interface PagedResult<T> {
  items: T[];
  nextCursor: string | null;
}

export interface ListAlertsQuery {
  status?: AlertStatus;
  severity?: AlertSeverity;
  deviceId?: string;
  cursor?: string;
  limit?: number;
}

export interface ListNotificationsQuery {
  unreadOnly?: boolean;
  cursor?: string;
  limit?: number;
}

const pool = createPool();

function normalizeLimit(limit?: number) {
  if (!limit || Number.isNaN(limit)) {
    return 50;
  }

  return Math.min(200, Math.max(1, Math.trunc(limit)));
}

function normalizeCursor(cursor?: string) {
  if (!cursor) {
    return undefined;
  }

  if (!/^\d+$/.test(cursor)) {
    return undefined;
  }

  return cursor;
}

function toIso(value: Date | string) {
  if (value instanceof Date) {
    return value.toISOString();
  }

  return new Date(value).toISOString();
}

function toAlertRecord(row: AlertRow): AlertRecord {
  return {
    id: String(row.id),
    deviceId: row.external_device_id,
    severity: row.severity,
    message: row.message,
    status: row.status,
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at)
  };
}

function toNotificationRecord(row: NotificationRow): NotificationRecord {
  return {
    id: String(row.id),
    alertId: row.alert_id === null ? null : String(row.alert_id),
    type: row.type,
    message: row.message,
    isRead: row.is_read,
    payload: typeof row.payload === "object" && row.payload !== null ? (row.payload as Record<string, unknown>) : {},
    createdAt: toIso(row.created_at)
  };
}

function addCondition(
  clauses: string[],
  values: DbValue[],
  sql: string,
  value: DbValue
) {
  clauses.push(sql.replace("?", `$${values.length + 1}`));
  values.push(value);
}

export async function listAlerts(query: ListAlertsQuery): Promise<PagedResult<AlertRecord>> {
  const clauses: string[] = [];
  const values: DbValue[] = [];

  if (query.status) {
    addCondition(clauses, values, "a.status = ?", query.status);
  }
  if (query.severity) {
    addCondition(clauses, values, "a.severity = ?", query.severity);
  }
  if (query.deviceId) {
    addCondition(clauses, values, "d.external_device_id = ?", query.deviceId);
  }

  const cursor = normalizeCursor(query.cursor);
  if (cursor) {
    clauses.push(`a.id < $${values.length + 1}::bigint`);
    values.push(cursor);
  }

  const limit = normalizeLimit(query.limit);
  const limitParam = `$${values.length + 1}`;
  values.push(limit + 1);

  const whereClause = clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";

  const result = await pool.query<AlertRow>(
    `
      SELECT
        a.id,
        d.external_device_id,
        a.severity,
        a.message,
        a.status,
        a.created_at,
        a.updated_at
      FROM alerts a
      INNER JOIN devices d ON d.id = a.device_id
      ${whereClause}
      ORDER BY a.id DESC
      LIMIT ${limitParam}
    `,
    values
  );

  const hasMore = result.rows.length > limit;
  const rows = hasMore ? result.rows.slice(0, limit) : result.rows;

  return {
    items: rows.map(toAlertRecord),
    nextCursor: hasMore ? String(rows[rows.length - 1]?.id ?? "") : null
  };
}

export async function listNotifications(query: ListNotificationsQuery): Promise<PagedResult<NotificationRecord>> {
  const clauses: string[] = [];
  const values: DbValue[] = [];

  if (query.unreadOnly) {
    addCondition(clauses, values, "n.is_read = ?", false);
  }

  const cursor = normalizeCursor(query.cursor);
  if (cursor) {
    clauses.push(`n.id < $${values.length + 1}::bigint`);
    values.push(cursor);
  }

  const limit = normalizeLimit(query.limit);
  const limitParam = `$${values.length + 1}`;
  values.push(limit + 1);

  const whereClause = clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";

  const result = await pool.query<NotificationRow>(
    `
      SELECT
        n.id,
        n.alert_id,
        n.type,
        n.message,
        n.is_read,
        n.payload,
        n.created_at
      FROM notifications n
      ${whereClause}
      ORDER BY n.id DESC
      LIMIT ${limitParam}
    `,
    values
  );

  const hasMore = result.rows.length > limit;
  const rows = hasMore ? result.rows.slice(0, limit) : result.rows;

  return {
    items: rows.map(toNotificationRecord),
    nextCursor: hasMore ? String(rows[rows.length - 1]?.id ?? "") : null
  };
}

export async function ackNotification(id: string): Promise<NotificationAckResponse | null> {
  if (!/^\d+$/.test(id)) {
    return null;
  }

  const result = await pool.query<NotificationRow>(
    `
      UPDATE notifications
      SET is_read = true,
          updated_at = now()
      WHERE id = $1::bigint
      RETURNING id, is_read, updated_at
    `,
    [id]
  );

  const row = result.rows[0];
  if (!row || !row.updated_at) {
    return null;
  }

  return {
    id: String(row.id),
    isRead: row.is_read,
    ackedAt: toIso(row.updated_at)
  };
}
