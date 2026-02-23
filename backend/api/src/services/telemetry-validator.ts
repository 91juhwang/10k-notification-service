import type { components } from "@microgrid/shared";

type TelemetryIngestRequest = components["schemas"]["TelemetryIngestRequest"];
type TelemetryBatchIngestRequest = components["schemas"]["TelemetryBatchIngestRequest"];
type TelemetryValidationResult =
  | {
      success: true;
      data: TelemetryIngestRequest;
    }
  | {
      success: false;
      errors: string[];
    };
type TelemetryBatchValidationResult =
  | {
      success: true;
      data: TelemetryBatchIngestRequest;
    }
  | {
      success: false;
      errors: string[];
    };

const telemetryFields = ["deviceId", "timestamp", "voltage", "frequency", "powerKw"] as const;
const telemetryFieldSet = new Set<string>(telemetryFields);

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function isIsoDateTime(value: unknown): value is string {
  return typeof value === "string" && !Number.isNaN(Date.parse(value));
}

function requiredFieldError(field: string) {
  return `Missing required field "${field}"`;
}

function invalidFieldError(field: string, expected: string) {
  return `Field "${field}" must be ${expected}`;
}

function isPresent(value: unknown) {
  return value !== undefined;
}

export function validateTelemetryPayload(payload: unknown): TelemetryValidationResult {
  if (!isObject(payload)) {
    return {
      success: false,
      errors: ["Payload must be a JSON object"]
    };
  }

  const errors: string[] = [];

  for (const field of Object.keys(payload)) {
    if (!telemetryFieldSet.has(field)) {
      errors.push(`Field "${field}" is not allowed`);
    }
  }

  const deviceId = payload.deviceId;
  if (!isPresent(deviceId)) {
    errors.push(requiredFieldError("deviceId"));
  } else if (typeof deviceId !== "string" || deviceId.trim().length === 0) {
    errors.push(invalidFieldError("deviceId", "a non-empty string"));
  }

  const timestamp = payload.timestamp;
  if (!isPresent(timestamp)) {
    errors.push(requiredFieldError("timestamp"));
  } else if (!isIsoDateTime(timestamp)) {
    errors.push(invalidFieldError("timestamp", "a valid date-time string"));
  }

  const voltage = payload.voltage;
  if (!isPresent(voltage)) {
    errors.push(requiredFieldError("voltage"));
  } else if (!isFiniteNumber(voltage)) {
    errors.push(invalidFieldError("voltage", "a finite number"));
  }

  const frequency = payload.frequency;
  if (!isPresent(frequency)) {
    errors.push(requiredFieldError("frequency"));
  } else if (!isFiniteNumber(frequency)) {
    errors.push(invalidFieldError("frequency", "a finite number"));
  }

  const powerKw = payload.powerKw;
  if (!isPresent(powerKw)) {
    errors.push(requiredFieldError("powerKw"));
  } else if (!isFiniteNumber(powerKw)) {
    errors.push(invalidFieldError("powerKw", "a finite number"));
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
      deviceId: deviceId as string,
      timestamp: timestamp as string,
      voltage: voltage as number,
      frequency: frequency as number,
      powerKw: powerKw as number
    }
  };
}

export function validateTelemetryBatchPayload(payload: unknown): TelemetryBatchValidationResult {
  if (!isObject(payload)) {
    return {
      success: false,
      errors: ["Payload must be a JSON object"]
    };
  }

  const errors: string[] = [];

  for (const field of Object.keys(payload)) {
    if (field !== "readings") {
      errors.push(`Field "${field}" is not allowed`);
    }
  }

  const readings = payload.readings;
  if (!Array.isArray(readings)) {
    errors.push('Field "readings" must be an array');
    return {
      success: false,
      errors
    };
  }

  if (readings.length === 0) {
    errors.push('Field "readings" must contain at least 1 item');
  }

  if (readings.length > 200) {
    errors.push('Field "readings" must contain at most 200 items');
  }

  const parsedReadings: TelemetryIngestRequest[] = [];
  readings.forEach((reading, index) => {
    const result = validateTelemetryPayload(reading);
    if (!result.success) {
      for (const error of result.errors) {
        errors.push(`readings[${index}]: ${error}`);
      }
      return;
    }

    parsedReadings.push(result.data);
  });

  if (errors.length > 0) {
    return {
      success: false,
      errors
    };
  }

  return {
    success: true,
    data: {
      readings: parsedReadings
    }
  };
}
