package config

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	flagHost            = "influxdb.host"
	flagDefaultLookback = "influxdb.default-lookback"

	// InfluxDB v1.x
	flagDatabase        = "influxdb.database"
	flagRetentionPolicy = "influxdb.retention-policy"
	flagUsername        = "influxdb.username"
	flagPassword        = "influxdb.password"
	flagUnsafeSsl       = "influxdb.unsafe_ssl"

	// InfluxDB v2.x
	flagToken        = "influxdb.token" // #nosec
	flagOrganization = "influxdb.organization"
	flagBucket       = "influxdb.bucket"

	defaultDefaultLookback = -168 * time.Hour
)

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

// AddFlags from this storage to the CLI
func (c *Configuration) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(&c.Host, flagHost, "", "InfluxDB host, eg http://localhost:8086 or http://localhost:9999")
	flagSet.DurationVar(&c.DefaultLookback, flagDefaultLookback, defaultDefaultLookback, "Lookback window for queries that do not specify lookback as parameter")

	flagSet.StringVar(&c.Database, flagDatabase, "", "InfluxDB database (v1.x only)")
	flagSet.StringVar(&c.RetentionPolicy, flagRetentionPolicy, "", "InfluxDB database (v1.x only)")
	flagSet.StringVar(&c.Username, flagUsername, "", "InfluxDB username (v1.x only)")
	flagSet.StringVar(&c.Password, flagPassword, "", "InfluxDB password (v1.x only)")
	flagSet.BoolVar(&c.UnsafeSsl, flagUnsafeSsl, false, "InfluxDB unsafe ssl (v1.x only)")

	flagSet.StringVar(&c.Token, flagToken, "", "InfluxDB token (v2.x only)")
	flagSet.StringVar(&c.Organization, flagOrganization, "", "InfluxDB organization (v2.x only)")
	flagSet.StringVar(&c.Bucket, flagBucket, "", "InfluxDB bucket (v2.x only)")
}

// InitFromViper initializes the options struct with values from Viper
func (c *Configuration) InitFromViper(v *viper.Viper) {
	c.Host = v.GetString(flagHost)
	c.DefaultLookback = v.GetDuration(flagDefaultLookback)

	c.Database = v.GetString(flagDatabase)
	c.RetentionPolicy = v.GetString(flagRetentionPolicy)
	c.Username = v.GetString(flagUsername)
	c.Password = v.GetString(flagPassword)
	c.UnsafeSsl = v.GetBool(flagUnsafeSsl)

	c.Token = v.GetString(flagToken)
	c.Organization = v.GetString(flagOrganization)
	c.Bucket = v.GetString(flagBucket)
}
