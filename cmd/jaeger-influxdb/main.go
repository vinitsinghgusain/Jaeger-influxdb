package main

import (
	"flag"
	"os"
	"path"

	"github.com/hashicorp/go-hclog"
	"github.com/influxdata/jaeger-influxdb/config"
	"github.com/influxdata/jaeger-influxdb/storev1"
	"github.com/influxdata/jaeger-influxdb/storev2"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var configPath string

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:            "jaeger-influxdb",
		Level:           hclog.Warn, // Jaeger only captures >= Warn, so don't bother logging below Warn
		IncludeLocation: true,
	})

	flag.StringVar(&configPath, "config", "", "The absolute path to the InfluxDB plugin's configuration file")
	flag.Parse()

	v := viper.New()
	if configPath != "" {
		v.SetConfigFile(path.Base(configPath))
		v.AddConfigPath(path.Dir(configPath))
	}
	err := v.ReadInConfig()
	if err != nil {
		logger.Error("failed to parse configuration file")
		os.Exit(1)
	}
	conf := config.Configuration{}
	conf.InitFromViper(v)

	var store shared.StoragePlugin
	var closeStore func() error

	if conf.Database != "" {
		logger.Warn("Started with InfluxDB v1")
		store, closeStore, err = storev1.NewStore(&conf, logger)
	} else if conf.Organization != "" && conf.Bucket != "" && conf.Token != "" {
		logger.Warn("Started with InfluxDB v2")
		store, closeStore, err = storev2.NewStore(&conf, logger)
	} else {
		err = errors.New("missing flags; for InfluxDB V1 set database and retention policy; for InfluxDB V2 set organization, bucket and token")
	}

	if err != nil {
		logger.Error("failed to open store", "error", err)
		os.Exit(1)
	}

	grpc.Serve(store)

	if err = closeStore(); err != nil {
		logger.Error("failed to close store", "error", err)
		os.Exit(1)
	}
}
