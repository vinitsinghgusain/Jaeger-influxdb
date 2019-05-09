package main

import (
	"flag"
	"path"
	"strings"

	"github.com/influxdata/jaeger-store/config"
	"github.com/influxdata/jaeger-store/storev1"
	"github.com/influxdata/jaeger-store/storev2"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var configPath string

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	if err := run(logger); err != nil {
		logger.Fatal(err.Error())
	}
}

func run(logger *zap.Logger) error {
	flag.StringVar(&configPath, "config", "", "A path to the InfluxDB plugin's configuration file")
	flag.Parse()

	if configPath != "" {
		viper.SetConfigFile(path.Base(configPath))
		viper.AddConfigPath(path.Dir(configPath))
	}

	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))

	conf := new(config.Configuration)
	conf.InitFromViper(v)

	var store shared.StoragePlugin
	var closeStore func() error
	var err error

	if conf.Database != "" && conf.RetentionPolicy != "" {
		store, closeStore, err = storev1.NewStore(conf, logger)
	} else if conf.Organization != "" && conf.Bucket != "" && conf.Token != "" {
		store, closeStore, err = storev2.NewStore(conf, logger)
	} else {
		err = errors.New("missing flags; for InfluxDB V1 set database and retention policy; for InfluxDB V2 set organization, bucket and token")
	}

	if err != nil {
		return errors.WithMessage(err, "failed to open store")
	}

	grpc.Serve(store)

	if err = closeStore(); err != nil {
		return errors.WithMessage(err, "failed to close store")
	}
	return nil
}
