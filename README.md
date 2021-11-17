# caddy-log-kafka

A Caddy log writer that writes logs to Kafka.

## Usage

This is a [Caddy log output module](https://caddyserver.com/docs/caddyfile/directives/log) so you can configure it in your `Caddyfile`:


```
example.com {
  log {
    output kafka {
      address kafka.default.svc.cluster.local:9092
      topic "com.example.caddy-log.{args.0}"
      partition 0
    }
    format json
  }
}
```

## Processing the logs

I use the following tools to analyze the logs:

- [ClickHouse](https://github.com/losfair/ops/blob/main/sqlscript/caddy_log_clickhouse.sql)
- [caddy-watch](https://github.com/losfair/caddy-watch)
