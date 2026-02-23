import type { components } from "@microgrid/shared";
import { createClient } from "redis";

type StreamEvent = components["schemas"]["StreamEvent"];
type StreamEventType = StreamEvent["eventType"];
type StreamListener = (event: StreamEvent) => void;

const streamEventTypes = new Set<StreamEventType>([
  "notification.created",
  "alert.created",
  "control.updated"
]);

function requiredEnv(name: string) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }

  return value;
}

function parseEvent(payload: string): StreamEvent | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(payload);
  } catch {
    return null;
  }

  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return null;
  }

  const eventType = (parsed as { eventType?: unknown }).eventType;
  const timestamp = (parsed as { timestamp?: unknown }).timestamp;
  const data = (parsed as { data?: unknown }).data;

  if (!streamEventTypes.has(eventType as StreamEventType)) {
    return null;
  }
  if (typeof timestamp !== "string" || Number.isNaN(Date.parse(timestamp))) {
    return null;
  }
  if (typeof data !== "object" || data === null || Array.isArray(data)) {
    return null;
  }

  return {
    eventType: eventType as StreamEventType,
    timestamp,
    data: data as Record<string, unknown>
  };
}

class RedisEventHub {
  private readonly client = createClient({
    url: requiredEnv("REDIS_URL")
  });
  private readonly listeners = new Set<StreamListener>();
  private started: Promise<void> | null = null;
  private readonly channel = process.env.REDIS_CHANNEL ?? "microgrid.events";

  constructor() {
    this.client.on("error", (error: unknown) => {
      console.error("[api] redis event hub error", error);
    });
  }

  private async ensureStarted() {
    if (!this.started) {
      this.started = (async () => {
        await this.client.connect();
        await this.client.subscribe(this.channel, (payload) => {
          const event = parseEvent(payload);
          if (!event) {
            return;
          }

          for (const listener of this.listeners) {
            listener(event);
          }
        });
      })();
    }

    return this.started;
  }

  async subscribe(listener: StreamListener) {
    await this.ensureStarted();
    this.listeners.add(listener);

    return () => {
      this.listeners.delete(listener);
    };
  }
}

let eventHub: RedisEventHub | null = null;

export function getRedisEventHub() {
  if (!eventHub) {
    eventHub = new RedisEventHub();
  }

  return eventHub;
}
