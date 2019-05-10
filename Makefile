VERSION?=$(shell git log --pretty=format:'%h' -n 1)

DOCKER_NAMESPACE?=influxdb
DOCKER_TAG?=latest

GOOS?=$(shell go env GOOS)

.PHONY: build
build:
	CGO_ENABLED=0 go build -o ./cmd/jaeger-influxdb/jaeger-influxdb-$(GOOS) ./cmd/jaeger-influxdb/main.go

.PHONY: build-linux
build-linux:
	GOOS=linux $(MAKE) build

.PHONY: docker
docker: docker-all-in-one docker-collector docker-query

.PHONY: docker-all-in-one
docker-all-in-one: build-linux
	docker build . -f ./cmd/jaeger-influxdb/Dockerfile.all-in-one -t $(DOCKER_NAMESPACE)/jaeger-all-in-one-influxdb:$(DOCKER_TAG)

.PHONY: docker-collector
docker-collector: build-linux
	docker build . -f ./cmd/jaeger-influxdb/Dockerfile.collector -t $(DOCKER_NAMESPACE)/jaeger-collector-influxdb:$(DOCKER_TAG)

.PHONY: docker-query
docker-query: build-linux
	docker build . -f ./cmd/jaeger-influxdb/Dockerfile.query -t $(DOCKER_NAMESPACE)/jaeger-query-influxdb:$(DOCKER_TAG)
