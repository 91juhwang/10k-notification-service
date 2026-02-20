import { randomUUID } from "node:crypto";
import cors from "cors";
import express from "express";
import type { Express } from "express";
import { requireIngestApiKey } from "./middleware/api-key.js";
import { enqueueTelemetryMessage } from "./services/sqs-producer.js";
import { validateTelemetryPayload } from "./services/telemetry-validator.js";

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
      phase: 2,
      message: "Telemetry ingestion endpoint is enabled."
    });
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

  return app;
}
