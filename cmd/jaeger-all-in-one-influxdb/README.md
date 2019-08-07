# All-in-One Example Container Image

This image is a quick way to try out Jaeger on InfluxDB.
The image includes:
- [Jaeger](https://www.jaegertracing.io/docs/architecture/) all-in-one binary (agent, collector, query)
- [InfluxDB](https://docs.influxdata.com/influxdb/)
- [Chronograf](https://docs.influxdata.com/chronograf/)
- [Hotrod](https://www.jaegertracing.io/docs/getting-started/#sample-app-hotrod) example app

## Build

```
$ make docker-all-in-one
```

## Run

For a simple desktop demo:

```
$ docker run -p 16686:16686 -p 8888:8888 -p 8080:8080 quay.io/influxdb/jaeger-all-in-one-influxdb:latest
```

The following ports are available:
- 16686 (HTTP): Jaeger UI
- 8086 (HTTP): InfluxDB service port
- 8888 (HTTP): Chronograf UI
- 8080 (HTTP): Hotrod UI
- 5775 (UDP): Zipkin/Thrift compact span listener
- 6831 (UDP): Jaeger/Thrift compact span listener
- 6832 (UDP): Jaeger/Thrift binary span listener
- 5778 (HTTP): Agent service (sampling endpoint proxy)
- 14268 (HTTP): Collector span listener
- 14250 (gRPC): Collector span listener

To run in a testing environment, you may need to mount the following directories as persistent volumes for data storage or configuration:
- /etc/supervisor: Supervisord configuration supervisord.conf
- /etc/jaeger: Sampling strategies configuration sampling_strategies.json
- /etc/influxdb: InfluxDB configuration influxdb.conf
- /var/lib/influxdb: InfluxDB data, WAL, indexes
- /var/lib/chronograf: Chronograf dashboard state

## Explore the Data

You may wish to explore the trace data directly, via the `influx` CLI, Chronograf, Grafana, etc.
Traces are stored in database `tracing`, retention policy `tracing`.
Spans are stored in measurement `span`, and span logs are stored in measurement `logs`.
