#!/usr/bin/env bash
set -euo pipefail

awslocal sqs create-queue --queue-name microgrid-telemetry-dlq >/dev/null
awslocal sqs create-queue --queue-name microgrid-control-dlq.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true >/dev/null

awslocal sqs create-queue --queue-name microgrid-telemetry >/dev/null
awslocal sqs create-queue --queue-name microgrid-control.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true >/dev/null

telemetry_queue_url="$(awslocal sqs get-queue-url --queue-name microgrid-telemetry --query QueueUrl --output text)"
control_queue_url="$(awslocal sqs get-queue-url --queue-name microgrid-control.fifo --query QueueUrl --output text)"
telemetry_dlq_url="$(awslocal sqs get-queue-url --queue-name microgrid-telemetry-dlq --query QueueUrl --output text)"
control_dlq_url="$(awslocal sqs get-queue-url --queue-name microgrid-control-dlq.fifo --query QueueUrl --output text)"

telemetry_dlq_arn="$(
  awslocal sqs get-queue-attributes --queue-url "$telemetry_dlq_url" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text
)"
control_dlq_arn="$(
  awslocal sqs get-queue-attributes --queue-url "$control_dlq_url" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text
)"

telemetry_redrive_policy="$(printf '{"deadLetterTargetArn":"%s","maxReceiveCount":"5"}' "$telemetry_dlq_arn")"
control_redrive_policy="$(printf '{"deadLetterTargetArn":"%s","maxReceiveCount":"5"}' "$control_dlq_arn")"

awslocal sqs set-queue-attributes --queue-url "$telemetry_queue_url" --attributes "RedrivePolicy=$telemetry_redrive_policy" >/dev/null
awslocal sqs set-queue-attributes --queue-url "$control_queue_url" --attributes "RedrivePolicy=$control_redrive_policy" >/dev/null

echo "LocalStack queues created (with DLQs and redrive policy)"
