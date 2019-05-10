This is the repository that contains InfluxDB Storage gRPC plugin for Jaeger.

> IMPORTANT: This plugin is still under development. We are using it internally
> already but the way we store data in InfluxDB can change based on what do we
> learn about the data structure!

The Jaeger community made a big work supporting external gRPC plugin to manage
integration with external backend without merge them as part of the Jaeger
Tracer. You can see issue [#1461](https://github.com/jaegertracing/jaeger/pull/1461).

This storage plugin will support InfluxDB v1 and v2 based on how you configure
it you will use the right client.

The plugin uses `go/mod` to manage its dependencies.

## Compile
In order to compile the plugin you can use `go build`:

```
GO111MODULE=on go run -o ./cmd/jaeger-influxdb ./cmd
```

## How it works

1. You need to checkout and build the last version of Jaeger (for testing purpose
you can use the `all-in-on`).

2. You also need to build the `influxdb-store` plugin, clone the repository and use:

```bash
go build ./cmd/jaeger-influxdb
```

3. You need to start InfluxDB, for this example we use InfluxDB v1. And you need
   to create a database. In this example the `tracing` database.

Using the env var `SPAN_STORAGE_TYPE=grpc-plugin` you can specify the storage
type, in this case the `grpc-plugin`.

When you switch to this particular storage plugin you get two new flags:

* `--grpc-storage-plugin.binary` needs to be pointed to the grpc plugin that
   you compiled just above.
* `--grpc-storage-plugin.configuration-file` needs to be pointed to a
   configuration file for the plugin. In our case there is a
   configuration file that works with InfluxDB v1 inside the repository
   `./config-example-v1.yaml`. You can use that one as test.

This is how the command looks like:

```bash
$ SPAN_STORAGE_TYPE=grpc-plugin jager-all-in-one-linux \
    --grpc-storage-plugin.binary .jaeger-influxdb \
    --grpc-storage-plugin.configuration-file ./config-example-v1.yaml

{"level":"info","ts":1557495654.945533,"caller":"flags/service.go:113","msg":"Mounting metrics handler on admin server","route":"/metrics"}
{"level":"info","ts":1557495654.9456794,"caller":"flags/admin.go:108","msg":"Mounting health check on admin server","route":"/"}
{"level":"info","ts":1557495654.945724,"caller":"flags/admin.go:114","msg":"Starting admin HTTP server","http-port":14269}
{"level":"info","ts":1557495654.9457393,"caller":"flags/admin.go:100","msg":"Admin server started","http-port":14269,"health-status":"unavailable"}
{"level":"info","ts":1557495654.9527798,"caller":"grpc/factory.go:67","msg":"External plugin storage configuration","configuration":{"PluginBinary":"/home/gianarb/git/jaeger-store/jaeger-influxdb","PluginConfigurationFile":"./config-example-v1.yaml"}}
```

Now you can visit [http://localhost:16686/](http://localhost:16686/). By default
Jaeger traces itself. So you should be able to see some traces and you can have
a look at the raw data in influxdb via cli:

```bash
$ influx
Connected to http://localhost:8086 version 1.7.1
InfluxDB shell version: 1.7.0~n201807090800
> use tracing
Using database tracing
> select * from span limit 5
name: span
time                duration flags operation_name                     process_tag_keys                       service_name span_id          tag:client-uuid    tag:component tag:hostname tag:http.method tag:http.status_code tag:http.url                                                                                                                tag:internal.span.format tag:ip          tag:jaeger.version tag:sampler.param tag:sampler.type tag:span.kind trace_id
----                -------- ----- --------------                     ----------------                       ------------ -------          ---------------    ------------- ------------ --------------- -------------------- ------------                                                                                                                ------------------------ ------          ------------------ ----------------- ---------------- ------------- --------
1557496661049626345 9010000  1     /api/traces                        jaeger.version,hostname,ip,client-uuid jaeger-query 1ab4fba76c4045f  s:3868643cf6b2b994 s:net/http    s:gianarb    s:GET           i:200                s:/api/traces?end=1557495664841000&limit=20&lookback=1h&maxDuration&minDuration&service=jaeger-query&start=1557492064841000 s:proto                  s:192.168.1.170 s:Go-2.15.1dev     b:t               s:const          s:server      1ab4fba76c4045f
1557496661061678565 4647000  1     /api/services                      jaeger.version,hostname,ip,client-uuid jaeger-query 75c7fc23fca70294 s:3868643cf6b2b994 s:net/http    s:gianarb    s:GET           i:200                s:/api/services                                                                                                             s:proto                  s:192.168.1.170 s:Go-2.15.1dev     b:t               s:const          s:server      75c7fc23fca70294
1557496661069655449 3081000  1     /api/services/{service}/operations jaeger.version,hostname,ip,client-uuid jaeger-query 41ad7ffe0f792295 s:3868643cf6b2b994 s:net/http    s:gianarb    s:GET           i:200                s:/api/services/jaeger-query/operations                                                                                     s:proto                  s:192.168.1.170 s:Go-2.15.1dev     b:t               s:const          s:server      41ad7ffe0f792295
1557496662064505047 3155000  1     /api/services                      jaeger.version,hostname,ip,client-uuid jaeger-query 7d0202924093a54d s:3868643cf6b2b994 s:net/http    s:gianarb    s:GET           i:200                s:/api/services                                                                                                             s:proto                  s:192.168.1.170 s:Go-2.15.1dev     b:t               s:const          s:server      7d0202924093a54d
1557496662067663425 2893000  1     /api/services/{service}/operations jaeger.version,hostname,ip,client-uuid jaeger-query 524f8d6ffce9132d s:3868643cf6b2b994 s:net/http    s:gianarb    s:GET           i:200                s:/api/services/jaeger-query/operations                                                                                     s:proto                  s:192.168.1.170 s:Go-2.15.1dev     b:t               s:const          s:server      524f8d6ffce9132d
```
