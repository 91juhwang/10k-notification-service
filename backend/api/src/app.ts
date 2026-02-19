import cors from "cors";
import express from "express";
import type { Express } from "express";

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
      phase: 0,
      message: "Phase 0 bootstrap complete. Phase 1 adds OpenAPI contract and real handlers."
    });
  });

  return app;
}
