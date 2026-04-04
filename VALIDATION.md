# Validation

## Automated Baseline

Current workspace baseline:

```bash
go test ./... -count=1
go vet ./...
```

Output:

```text
ok   github.com/go-lynx/lynx-rabbitmq       (passes root package unit tests)
?    github.com/go-lynx/lynx-rabbitmq/conf  [no test files]
go vet ./...                                (passes)
```

## What This Means

- The root package now has committed unit tests covering default config construction, runtime config scanning, initialization-time defaulting/validation, retry behavior, metrics, and manager helpers.
- The `conf` package is generated protobuf code and still has no standalone test files.
- Publish, subscribe, exchange/queue declaration, and reconnect behavior are still not covered by broker-backed integration tests in this repository.

## Recommended Manual Smoke Checks

- Start against a reachable RabbitMQ broker and verify one producer can publish to a declared exchange.
- Verify one consumer receives, handles, and acknowledges a message from the configured queue.
- If retry or health monitoring is enabled in your deployment, verify those paths manually after a forced broker disconnect.
