import type { NextFunction, Request, Response } from "express";

export function requireIngestApiKey(req: Request, res: Response, next: NextFunction) {
  const expectedApiKey = process.env.INGEST_API_KEY;
  if (!expectedApiKey) {
    res.status(500).json({ message: "INGEST_API_KEY is not configured" });
    return;
  }

  const providedApiKey = req.header("x-api-key");
  if (!providedApiKey || providedApiKey !== expectedApiKey) {
    res.status(401).json({ message: "Unauthorized" });
    return;
  }

  next();
}
