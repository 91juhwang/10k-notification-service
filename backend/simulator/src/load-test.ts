import { performance } from "node:perf_hooks";

interface LoadTestConfig {
  endpoint: string;
  apiKey: string;
  ratePerMinute: number;
  durationSeconds: number;
  deviceCount: number;
  useBatch: boolean;
  batchSize: number;
  maxInFlight: number;
  requestTimeoutMs: number;
  minSuccessRate: number;
  maxP95LatencyMs: number;
}

interface LoadMetrics {
  startedAtMs: number;
  attemptedRequests: number;
  acceptedRequests: number;
  attemptedMessages: number;
  acceptedMessages: number;
  rejected4xx: number;
  rejected5xx: number;
  networkErrors: number;
  latencySamplesMs: number[];
}

const MAX_LATENCY_SAMPLES = 5000;

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

function readFloatEnv(name: string, fallback: number, { min, max }: { min: number; max: number }) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }

  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < min || parsed > max) {
    throw new Error(`${name} must be a number between ${min} and ${max}`);
  }

  return parsed;
}

function readBooleanEnv(name: string, fallback: boolean) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }

  const normalized = raw.trim().toLowerCase();
  if (normalized === "true") {
    return true;
  }
  if (normalized === "false") {
    return false;
  }

  throw new Error(`${name} must be "true" or "false"`);
}

function readConfig(): LoadTestConfig {
  const apiBaseUrl = process.env.API_BASE_URL ?? "http://localhost:4000";
  const apiKey = process.env.INGEST_API_KEY ?? "dev-ingest-key";

  return {
    endpoint: readBooleanEnv("SIM_LOAD_USE_BATCH", false)
      ? `${apiBaseUrl}/api/v1/telemetry/batch`
      : `${apiBaseUrl}/api/v1/telemetry`,
    apiKey,
    ratePerMinute: readIntEnv("SIM_LOAD_RATE_PER_MINUTE", 10_000, { min: 1, max: 500_000 }),
    durationSeconds: readIntEnv("SIM_LOAD_DURATION_SECONDS", 300, { min: 1, max: 86_400 }),
    deviceCount: readIntEnv("SIM_LOAD_DEVICE_COUNT", 120, { min: 1, max: 10_000 }),
    useBatch: readBooleanEnv("SIM_LOAD_USE_BATCH", false),
    batchSize: readIntEnv("SIM_LOAD_BATCH_SIZE", 50, { min: 1, max: 200 }),
    maxInFlight: readIntEnv("SIM_LOAD_MAX_IN_FLIGHT", 300, { min: 1, max: 20_000 }),
    requestTimeoutMs: readIntEnv("SIM_LOAD_TIMEOUT_MS", 10_000, { min: 100, max: 120_000 }),
    minSuccessRate: readFloatEnv("SIM_SLO_MIN_SUCCESS_RATE", 0.99, { min: 0, max: 1 }),
    maxP95LatencyMs: readFloatEnv("SIM_SLO_MAX_P95_LATENCY_MS", 1500, { min: 1, max: 120_000 })
  };
}

function randomInRange(min: number, max: number) {
  return Math.random() * (max - min) + min;
}

function buildTelemetryBody(sequence: number, deviceCount: number) {
  const deviceIndex = sequence % deviceCount;

  return {
    deviceId: `sim-device-${deviceIndex}`,
    timestamp: new Date().toISOString(),
    voltage: Number(randomInRange(190, 260).toFixed(3)),
    frequency: Number(randomInRange(58, 62).toFixed(3)),
    powerKw: Number(randomInRange(20, 140).toFixed(3))
  };
}

function pushLatencySample(samples: number[], value: number) {
  if (!Number.isFinite(value) || value < 0) {
    return;
  }

  if (samples.length >= MAX_LATENCY_SAMPLES) {
    samples.shift();
  }

  samples.push(value);
}

function percentile(samples: number[], ratio: number) {
  if (samples.length === 0) {
    return null;
  }

  const sorted = [...samples].sort((left, right) => left - right);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
  return sorted[index];
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function sendTelemetryRequest(config: LoadTestConfig, metrics: LoadMetrics, bodies: ReturnType<typeof buildTelemetryBody>[]) {
  const start = performance.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.requestTimeoutMs);
  const messageCount = bodies.length;

  metrics.attemptedRequests += 1;
  metrics.attemptedMessages += messageCount;

  try {
    const response = await fetch(config.endpoint, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": config.apiKey
      },
      body: JSON.stringify(config.useBatch ? { readings: bodies } : bodies[0]),
      signal: controller.signal
    });

    const latencyMs = performance.now() - start;
    pushLatencySample(metrics.latencySamplesMs, latencyMs);

    if (response.status >= 200 && response.status < 300) {
      metrics.acceptedRequests += 1;
      if (config.useBatch) {
        const payload = (await response.json().catch(() => null)) as { acceptedCount?: unknown } | null;
        const acceptedCount =
          payload && typeof payload.acceptedCount === "number" && Number.isInteger(payload.acceptedCount)
            ? payload.acceptedCount
            : messageCount;
        metrics.acceptedMessages += acceptedCount;
      } else {
        metrics.acceptedMessages += 1;
      }
      return;
    }

    if (response.status >= 500) {
      metrics.rejected5xx += 1;
      return;
    }

    metrics.rejected4xx += 1;
  } catch {
    metrics.networkErrors += 1;
  } finally {
    clearTimeout(timeout);
  }
}

function summarizeMetrics(config: LoadTestConfig, metrics: LoadMetrics) {
  const elapsedSeconds = Math.max(1, (Date.now() - metrics.startedAtMs) / 1000);
  const successRate = metrics.attemptedMessages === 0 ? 0 : metrics.acceptedMessages / metrics.attemptedMessages;
  const throughputPerMinute = (metrics.acceptedMessages / elapsedSeconds) * 60;
  const p50Ms = percentile(metrics.latencySamplesMs, 0.5);
  const p95Ms = percentile(metrics.latencySamplesMs, 0.95);

  const summary = {
    targetRatePerMinute: config.ratePerMinute,
    transportMode: config.useBatch ? "batch" : "single",
    batchSize: config.useBatch ? config.batchSize : 1,
    actualAcceptedPerMinute: Number(throughputPerMinute.toFixed(2)),
    attemptedRequests: metrics.attemptedRequests,
    acceptedRequests: metrics.acceptedRequests,
    attemptedMessages: metrics.attemptedMessages,
    acceptedMessages: metrics.acceptedMessages,
    rejected4xx: metrics.rejected4xx,
    rejected5xx: metrics.rejected5xx,
    networkErrors: metrics.networkErrors,
    successRate: Number(successRate.toFixed(4)),
    latencyP50Ms: p50Ms === null ? null : Number(p50Ms.toFixed(2)),
    latencyP95Ms: p95Ms === null ? null : Number(p95Ms.toFixed(2)),
    sampleCount: metrics.latencySamplesMs.length,
    durationSeconds: Number(elapsedSeconds.toFixed(2))
  };

  const meetsSuccessRateSlo = successRate >= config.minSuccessRate;
  const meetsP95LatencySlo = p95Ms !== null && p95Ms <= config.maxP95LatencyMs;

  return {
    summary,
    meetsSlo: meetsSuccessRateSlo && meetsP95LatencySlo,
    sloTargets: {
      minSuccessRate: config.minSuccessRate,
      maxP95LatencyMs: config.maxP95LatencyMs
    }
  };
}

async function runLoadTest() {
  const config = readConfig();
  const metrics: LoadMetrics = {
    startedAtMs: Date.now(),
    attemptedRequests: 0,
    acceptedRequests: 0,
    attemptedMessages: 0,
    acceptedMessages: 0,
    rejected4xx: 0,
    rejected5xx: 0,
    networkErrors: 0,
    latencySamplesMs: []
  };

  const targetMessages = Math.round((config.ratePerMinute / 60) * config.durationSeconds);
  const messagesPerSecond = config.ratePerMinute / 60;
  const inFlight = new Set<Promise<void>>();

  console.log("[simulator] load test started", {
    endpoint: config.endpoint,
    targetMessages,
    ratePerMinute: config.ratePerMinute,
    durationSeconds: config.durationSeconds,
    maxInFlight: config.maxInFlight,
    transportMode: config.useBatch ? "batch" : "single",
    batchSize: config.useBatch ? config.batchSize : 1
  });

  const startMs = Date.now();
  const endMs = startMs + config.durationSeconds * 1000;
  let carry = 0;
  let sequence = 0;
  let nextTickMs = startMs;

  while (Date.now() < endMs) {
    nextTickMs += 1000;
    carry += messagesPerSecond;
    const toDispatch = Math.floor(carry);
    carry -= toDispatch;

    if (config.useBatch) {
      let remaining = toDispatch;
      while (remaining > 0) {
        while (inFlight.size >= config.maxInFlight) {
          await Promise.race(inFlight);
        }

        const currentBatchSize = Math.min(config.batchSize, remaining);
        const bodies = Array.from({ length: currentBatchSize }, () => {
          const body = buildTelemetryBody(sequence, config.deviceCount);
          sequence += 1;
          return body;
        });

        const task = sendTelemetryRequest(config, metrics, bodies);
        inFlight.add(task);
        task.finally(() => {
          inFlight.delete(task);
        });

        remaining -= currentBatchSize;
      }
    } else {
      for (let index = 0; index < toDispatch; index += 1) {
        while (inFlight.size >= config.maxInFlight) {
          await Promise.race(inFlight);
        }

        const task = sendTelemetryRequest(config, metrics, [buildTelemetryBody(sequence, config.deviceCount)]);
        sequence += 1;
        inFlight.add(task);
        task.finally(() => {
          inFlight.delete(task);
        });
      }
    }

    const waitMs = nextTickMs - Date.now();
    if (waitMs > 0) {
      await sleep(waitMs);
    }
  }

  if (carry >= 1) {
    const toDispatch = Math.floor(carry);
    if (config.useBatch) {
      let remaining = toDispatch;
      while (remaining > 0) {
        while (inFlight.size >= config.maxInFlight) {
          await Promise.race(inFlight);
        }

        const currentBatchSize = Math.min(config.batchSize, remaining);
        const bodies = Array.from({ length: currentBatchSize }, () => {
          const body = buildTelemetryBody(sequence, config.deviceCount);
          sequence += 1;
          return body;
        });

        const task = sendTelemetryRequest(config, metrics, bodies);
        inFlight.add(task);
        task.finally(() => {
          inFlight.delete(task);
        });

        remaining -= currentBatchSize;
      }
    } else {
      for (let index = 0; index < toDispatch; index += 1) {
        while (inFlight.size >= config.maxInFlight) {
          await Promise.race(inFlight);
        }

        const task = sendTelemetryRequest(config, metrics, [buildTelemetryBody(sequence, config.deviceCount)]);
        sequence += 1;
        inFlight.add(task);
        task.finally(() => {
          inFlight.delete(task);
        });
      }
    }
  }

  await Promise.allSettled(inFlight);

  const result = summarizeMetrics(config, metrics);

  console.log("[simulator] load test summary", result.summary);
  console.log("[simulator] slo targets", result.sloTargets);

  if (!result.meetsSlo) {
    console.error("[simulator] slo check failed");
    process.exitCode = 1;
    return;
  }

  console.log("[simulator] slo check passed");
}

runLoadTest().catch((error) => {
  console.error("[simulator] load test failed", error);
  process.exitCode = 1;
});
