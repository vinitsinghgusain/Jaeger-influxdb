package jaeger_influxdb

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/influxdata/jaeger-store/config"
	"github.com/influxdata/jaeger-store/storev1"
	"github.com/influxdata/jaeger-store/storev2"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var configPath string

func main() {
	conf := new(config.Configuration)

	flagSet := pflag.NewFlagSet("jaeger-influxdb", pflag.ExitOnError)
	flagSet.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	conf.AddFlags(flagSet)
	pflag.Parse()

	if configPath != "" {
		viper.SetConfigFile(path.Base(configPath))
		viper.AddConfigPath(path.Dir(configPath))
	}

	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	var store shared.StoragePlugin
	if conf.Database != "" && conf.RetentionPolicy != "" {
		store, err = storev1.NewStore(conf, logger)
	} else if conf.Organization != "" && conf.Bucket != "" && conf.Token != "" {
		store, err = storev2.NewStore(conf, logger)
	} else {
		err = errors.New("missing flags; for InfluxDB V1 set database and retention policy; for InfluxDB V2 set organization, bucket and token")
	}

	if err != nil {
		if _, outerErr := fmt.Fprintln(os.Stderr, err); outerErr != nil {
			panic(outerErr)
		}
		os.Exit(1)
	}

	grpc.Serve(store)
}
