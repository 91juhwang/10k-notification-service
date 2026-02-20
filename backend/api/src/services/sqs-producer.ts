import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import type { components } from "@microgrid/shared";

type TelemetryIngestRequest = components["schemas"]["TelemetryIngestRequest"];

export interface TelemetryQueueMessage extends TelemetryIngestRequest {
  ingestId: string;
  queuedAt: string;
}

function requiredEnv(name: string) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }

  return value;
}

function createSqsClient() {
  const region = process.env.AWS_REGION ?? "us-east-1";
  const endpoint = process.env.SQS_ENDPOINT;
  const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
  const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;

  return new SQSClient({
    region,
    endpoint,
    credentials:
      accessKeyId && secretAccessKey
        ? {
            accessKeyId,
            secretAccessKey
          }
        : undefined
  });
}

const sqsClient = createSqsClient();

export async function enqueueTelemetryMessage(message: TelemetryQueueMessage) {
  const queueUrl = requiredEnv("TELEMETRY_QUEUE_URL");

  const result = await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(message)
    })
  );

  return {
    queueMessageId: result.MessageId ?? null
  };
}
