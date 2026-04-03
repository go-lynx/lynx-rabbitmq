# Validation

## Automated Baseline

Current workspace baseline:

```bash
go test ./...
```

Output:

```text
?    github.com/go-lynx/lynx-rabbitmq       [no test files]
?    github.com/go-lynx/lynx-rabbitmq/conf  [no test files]
```

## What This Means

- This module currently has no committed Go test files.
- Publish, subscribe, retry, queue declaration, and health behavior are not covered by automated Go tests yet.

## Recommended Manual Smoke Checks

- Start against a reachable RabbitMQ broker and verify one producer can publish to a declared exchange.
- Verify one consumer receives, handles, and acknowledges a message from the configured queue.
- If retry or health monitoring is enabled in your deployment, verify those paths manually after a forced broker disconnect.
