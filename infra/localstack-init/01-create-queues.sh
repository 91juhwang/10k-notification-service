#!/usr/bin/env bash
set -euo pipefail

awslocal sqs create-queue --queue-name microgrid-telemetry >/dev/null
awslocal sqs create-queue --queue-name microgrid-control.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true >/dev/null

echo "LocalStack queues created"
