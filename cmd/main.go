package main

import (
	"flag"
	"path"
	"strings"
	"time"

	"github.com/spf13/viper"

	influxdbv1 "github.com/influxdata/jaeger-store/v1"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
)

var configPath string

func main() {
	flag.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	flag.Parse()

	if configPath != "" {
		viper.SetConfigFile(path.Base(configPath))
		viper.AddConfigPath(path.Dir(configPath))
	}

	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))

	opts := &Configuration{}
	opts.InitFromViper(v)

	// TODO: Replace this with v1 or v2 depending on how it is configured
	grpc.Serve(&influxdbv1.V1Store{})
}

// Configuration describes the options to customize the storage behavior
type Configuration struct {
	Host            string        `yaml:"host"`
	DefaultLookback time.Duration `yaml:"default_lookback"`

	// InfluxDB v1.x
	Database        string `yaml:"database"`
	RetentionPolicy string `yaml:"retention_policy"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	UnsafeSsl       bool   `yaml:"unsafe_ssl"`

	// InfluxDB v2.x
	Token        string `yaml:"token"`
	Organization string `yaml:"organization"`
	Bucket       string `yaml:"bucket"`
}

// InitFromViper initializes the options struct with values from Viper
func (config *Configuration) InitFromViper(v *viper.Viper) {
}
