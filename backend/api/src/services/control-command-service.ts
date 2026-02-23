import { createPool } from "@microgrid/db";
import type { components } from "@microgrid/shared";

type ControlCommandRequest = components["schemas"]["ControlCommandRequest"];
type ControlCommandRecord = components["schemas"]["ControlCommand"];
type ControlCommandStatus = ControlCommandRecord["status"];

interface ControlCommandRow {
  id: string;
  external_device_id: string;
  command_type: string;
  payload: unknown;
  status: ControlCommandStatus;
  idempotency_key: string;
  requested_by: string | null;
  created_at: Date | string;
  updated_at: Date | string;
}

interface ValidationSuccess {
  success: true;
  data: ControlCommandRequest;
}

interface ValidationFailure {
  success: false;
  errors: string[];
}

type ValidationResult = ValidationSuccess | ValidationFailure;

interface CreateControlCommandInput extends ControlCommandRequest {
  commandId: string;
  requestedBy: string | null;
}

interface CreateControlCommandResultCreated {
  created: true;
  command: ControlCommandRecord;
}

interface CreateControlCommandResultConflict {
  created: false;
}

type CreateControlCommandResult = CreateControlCommandResultCreated | CreateControlCommandResultConflict;

const controlStatuses = new Set<ControlCommandStatus>(["queued", "processing", "succeeded", "failed", "cancelled"]);
const pool = createPool();

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isCommandStatus(value: unknown): value is ControlCommandStatus {
  return typeof value === "string" && controlStatuses.has(value as ControlCommandStatus);
}

function toIso(value: Date | string) {
  if (value instanceof Date) {
    return value.toISOString();
  }

  return new Date(value).toISOString();
}

function toControlCommand(row: ControlCommandRow): ControlCommandRecord {
  return {
    id: row.id,
    deviceId: row.external_device_id,
    commandType: row.command_type,
    payload: isObject(row.payload) ? row.payload : {},
    status: row.status,
    idempotencyKey: row.idempotency_key,
    requestedBy: row.requested_by,
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at)
  };
}

function requiredFieldError(field: string) {
  return `Missing required field \"${field}\"`;
}

function invalidFieldError(field: string, expected: string) {
  return `Field \"${field}\" must be ${expected}`;
}

export function validateControlCommandPayload(payload: unknown): ValidationResult {
  if (!isObject(payload)) {
    return {
      success: false,
      errors: ["Payload must be a JSON object"]
    };
  }

  const errors: string[] = [];

  if (!Object.prototype.hasOwnProperty.call(payload, "deviceId")) {
    errors.push(requiredFieldError("deviceId"));
  } else if (typeof payload.deviceId !== "string" || payload.deviceId.trim().length === 0) {
    errors.push(invalidFieldError("deviceId", "a non-empty string"));
  }

  if (!Object.prototype.hasOwnProperty.call(payload, "commandType")) {
    errors.push(requiredFieldError("commandType"));
  } else if (typeof payload.commandType !== "string" || payload.commandType.trim().length === 0) {
    errors.push(invalidFieldError("commandType", "a non-empty string"));
  }

  if (!Object.prototype.hasOwnProperty.call(payload, "payload")) {
    errors.push(requiredFieldError("payload"));
  } else if (!isObject(payload.payload)) {
    errors.push(invalidFieldError("payload", "an object"));
  }

  if (!Object.prototype.hasOwnProperty.call(payload, "idempotencyKey")) {
    errors.push(requiredFieldError("idempotencyKey"));
  } else if (typeof payload.idempotencyKey !== "string" || payload.idempotencyKey.trim().length === 0) {
    errors.push(invalidFieldError("idempotencyKey", "a non-empty string"));
  }

  const allowedFields = new Set(["deviceId", "commandType", "payload", "idempotencyKey"]);
  for (const field of Object.keys(payload)) {
    if (!allowedFields.has(field)) {
      errors.push(`Field \"${field}\" is not allowed`);
    }
  }

  if (errors.length > 0) {
    return {
      success: false,
      errors
    };
  }

  return {
    success: true,
    data: {
      deviceId: payload.deviceId as string,
      commandType: payload.commandType as string,
      payload: payload.payload as Record<string, unknown>,
      idempotencyKey: payload.idempotencyKey as string
    }
  };
}

export async function createQueuedControlCommand(input: CreateControlCommandInput): Promise<CreateControlCommandResult> {
  const result = await pool.query<ControlCommandRow>(
    `
      INSERT INTO control_commands (
        id,
        external_device_id,
        command_type,
        payload,
        status,
        idempotency_key,
        requested_by
      )
      VALUES ($1, $2, $3, $4::jsonb, 'queued', $5, $6)
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING
        id,
        external_device_id,
        command_type,
        payload,
        status,
        idempotency_key,
        requested_by,
        created_at,
        updated_at
    `,
    [
      input.commandId,
      input.deviceId,
      input.commandType,
      JSON.stringify(input.payload),
      input.idempotencyKey,
      input.requestedBy
    ]
  );

  const row = result.rows[0];
  if (!row || !isCommandStatus(row.status)) {
    return {
      created: false
    };
  }

  return {
    created: true,
    command: toControlCommand(row)
  };
}

export async function getControlCommandById(id: string): Promise<ControlCommandRecord | null> {
  if (typeof id !== "string" || id.length === 0) {
    return null;
  }

  const result = await pool.query<ControlCommandRow>(
    `
      SELECT
        id,
        external_device_id,
        command_type,
        payload,
        status,
        idempotency_key,
        requested_by,
        created_at,
        updated_at
      FROM control_commands
      WHERE id = $1
      LIMIT 1
    `,
    [id]
  );

  const row = result.rows[0];
  if (!row || !isCommandStatus(row.status)) {
    return null;
  }

  return toControlCommand(row);
}

export async function deleteControlCommandById(id: string) {
  await pool.query("DELETE FROM control_commands WHERE id = $1", [id]);
}
