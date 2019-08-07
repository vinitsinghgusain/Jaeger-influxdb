#!/bin/sh

set -ex

until influx -execute 'SHOW DATABASES'; do
  echo "waiting for InfluxDB"
  sleep 2
done
influx -execute "CREATE DATABASE \"${INFLUXDB_DATABASE}\" WITH DURATION ${INFLUXDB_RETENTION_HOURS}h SHARD DURATION ${INFLUXDB_SHARD_DURATION_HOURS}h NAME ${INFLUXDB_RETENTION_POLICY}" || /bin/true

/usr/local/bin/jaeger-all-in-one
