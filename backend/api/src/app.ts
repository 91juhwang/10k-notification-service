import { randomUUID } from "node:crypto";
import cors from "cors";
import express from "express";
import type { Express } from "express";
import type { components } from "@microgrid/shared";
import { getAuthContext, requireBearerAuth, requireRoles } from "./middleware/bearer-auth.js";
import { requireIngestApiKey } from "./middleware/api-key.js";
import { authenticateUser, issueLoginToken } from "./services/auth-service.js";
import { ackNotification, listAlerts, listNotifications } from "./services/dashboard-repository.js";
import { getRedisEventHub } from "./services/redis-event-hub.js";
import { enqueueTelemetryMessage } from "./services/sqs-producer.js";
import { validateTelemetryPayload } from "./services/telemetry-validator.js";

type AlertRecord = components["schemas"]["Alert"];
type AlertStatus = AlertRecord["status"];
type AlertSeverity = AlertRecord["severity"];
type StreamEvent = components["schemas"]["StreamEvent"];

const alertStatusSet = new Set<AlertStatus>(["OPEN", "ACKED", "RESOLVED"]);
const alertSeveritySet = new Set<AlertSeverity>(["LOW", "MEDIUM", "HIGH", "CRITICAL"]);

function readQueryString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function readQueryNumber(value: unknown) {
  if (typeof value !== "string") {
    return undefined;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return undefined;
  }

  return parsed;
}

function readQueryBoolean(value: unknown) {
  if (value === "true") {
    return true;
  }
  if (value === "false") {
    return false;
  }

  return undefined;
}

function parseAlertStatus(value: unknown) {
  const status = readQueryString(value);
  if (!status || !alertStatusSet.has(status as AlertStatus)) {
    return undefined;
  }

  return status as AlertStatus;
}

function parseAlertSeverity(value: unknown) {
  const severity = readQueryString(value);
  if (!severity || !alertSeveritySet.has(severity as AlertSeverity)) {
    return undefined;
  }

  return severity as AlertSeverity;
}

function readLoginCredentials(body: unknown) {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return null;
  }

  const username = (body as { username?: unknown }).username;
  const password = (body as { password?: unknown }).password;

  if (typeof username !== "string" || typeof password !== "string") {
    return null;
  }

  if (username.trim().length === 0 || password.length === 0) {
    return null;
  }

  return { username, password };
}

export function createApp(): Express {
  const app = express();

  app.use(cors());
  app.use(express.json({ limit: "256kb" }));

  app.get("/health", (_req, res) => {
    res.json({ status: "ok", service: "api" });
  });

  app.get("/", (_req, res) => {
    res.json({
      name: "microgrid-api",
      phase: 4,
      message: "Auth, list APIs, and realtime stream are enabled."
    });
  });

  app.post("/api/v1/auth/login", (req, res) => {
    const credentials = readLoginCredentials(req.body);
    if (!credentials) {
      res.status(401).json({ message: "Invalid credentials" });
      return;
    }

    try {
      const authUser = authenticateUser(credentials.username, credentials.password);
      if (!authUser) {
        res.status(401).json({ message: "Invalid credentials" });
        return;
      }

      const loginResult = issueLoginToken(authUser);
      res.json({
        accessToken: loginResult.accessToken,
        tokenType: "Bearer",
        expiresIn: loginResult.expiresIn,
        role: loginResult.role
      });
    } catch (error) {
      console.error("[api] auth login failed", error);
      res.status(500).json({ message: "Authentication unavailable" });
    }
  });

  app.post("/api/v1/telemetry", requireIngestApiKey, async (req, res) => {
    const validationResult = validateTelemetryPayload(req.body);

    if (!validationResult.success) {
      res.status(400).json({
        message: "Invalid telemetry payload",
        details: validationResult.errors
      });
      return;
    }

    const ingestId = randomUUID();
    const queuedAt = new Date().toISOString();

    try {
      const queueResult = await enqueueTelemetryMessage({
        ingestId,
        queuedAt,
        ...validationResult.data
      });

      res.status(202).json({
        ingestId,
        status: "accepted",
        queuedAt,
        queueMessageId: queueResult.queueMessageId
      });
    } catch (error) {
      console.error("[api] failed to enqueue telemetry", error);
      res.status(503).json({ message: "Telemetry queue unavailable" });
    }
  });

  app.get("/api/v1/alerts", requireBearerAuth, async (req, res) => {
    try {
      const result = await listAlerts({
        status: parseAlertStatus(req.query.status),
        severity: parseAlertSeverity(req.query.severity),
        deviceId: readQueryString(req.query.deviceId),
        cursor: readQueryString(req.query.cursor),
        limit: readQueryNumber(req.query.limit)
      });

      res.json(result);
    } catch (error) {
      console.error("[api] failed to list alerts", error);
      res.status(503).json({ message: "Alerts unavailable" });
    }
  });

  app.get("/api/v1/notifications", requireBearerAuth, async (req, res) => {
    try {
      const result = await listNotifications({
        unreadOnly: readQueryBoolean(req.query.unreadOnly),
        cursor: readQueryString(req.query.cursor),
        limit: readQueryNumber(req.query.limit)
      });

      res.json(result);
    } catch (error) {
      console.error("[api] failed to list notifications", error);
      res.status(503).json({ message: "Notifications unavailable" });
    }
  });

  app.post("/api/v1/notifications/:id/ack", requireBearerAuth, requireRoles(["operator", "admin"]), async (req, res) => {
    try {
      const notificationId = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;
      if (!notificationId) {
        res.status(404).json({ message: "Notification not found" });
        return;
      }

      const ackResult = await ackNotification(notificationId);
      if (!ackResult) {
        res.status(404).json({ message: "Notification not found" });
        return;
      }

      res.json(ackResult);
    } catch (error) {
      console.error("[api] failed to acknowledge notification", error);
      res.status(503).json({ message: "Notification ack unavailable" });
    }
  });

  app.get("/api/v1/stream", requireBearerAuth, async (req, res) => {
    const authContext = getAuthContext(res);
    if (!authContext) {
      res.status(401).json({ message: "Unauthorized" });
      return;
    }

    const streamHub = getRedisEventHub();
    let ready = false;

    const sendEvent = (event: StreamEvent) => {
      if (!ready) {
        return;
      }

      res.write(`event: ${event.eventType}\n`);
      res.write(`data: ${JSON.stringify(event)}\n\n`);
    };

    let unsubscribe: (() => void) | null = null;
    try {
      unsubscribe = await streamHub.subscribe(sendEvent);
    } catch (error) {
      console.error("[api] stream subscription failed", error);
      res.status(503).json({ message: "Event stream unavailable" });
      return;
    }

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders();
    ready = true;

    res.write(`: stream-open role=${authContext.role}\n\n`);

    const keepAlive = setInterval(() => {
      res.write(": ping\n\n");
    }, 25000);

    req.on("close", () => {
      clearInterval(keepAlive);
      unsubscribe?.();
    });
  });

  return app;
}
