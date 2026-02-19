import { z } from "zod";

export const telemetrySchema = z.object({
  deviceId: z.string().min(1),
  timestamp: z.string().datetime({ offset: true }),
  voltage: z.number().finite(),
  frequency: z.number().finite(),
  powerKw: z.number().finite()
});

export type TelemetryPayload = z.infer<typeof telemetrySchema>;
