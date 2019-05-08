This is the repository that contains InfluxDB Storage gRPC plugin for Jaeger.

The Jaeger community made a big work supporting external gRPC plugin to manage
integration with external backend without merge them as part of the Jaeger
Tracer. You can see issue [#1461](https://github.com/jaegertracing/jaeger/pull/1461).

This storage plugin will support InfluxDB v1 and v2 based on how you configure
it you will use the right client.

The plugin uses `go/mod` to manage its dependencies.

## Compile
In order to compile the plugin you can use `go build`:

```
GO111MODULE=on go run -o ./jaeger-store ./cmd
```

## How it works

You need to checkout and build the last version of Jaeger (for testing purpose
you can use the `all-in-on`).

```
SPAN_STORAGE_TYPE=grpc-plugin ./cmd/all-in-one/all-in-one-linux \
    --grpc-storage-plugin.binary ./path/jaeger-store/binary
```

The binary supports a YAML config file. It can be passed adding the flag
`--grpc-storage-plugin.configuration-file string`.
