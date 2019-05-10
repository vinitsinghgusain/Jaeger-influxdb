package config

import (
	"time"

	"github.com/spf13/viper"
)

const (
	flagHost            = "host"
	flagDefaultLookback = "default-lookback"

	// InfluxDB v1.x
	flagDatabase        = "database"
	flagRetentionPolicy = "retention-policy"
	flagUsername        = "username"
	flagPassword        = "password"
	flagUnsafeSsl       = "unsafe_ssl"

	// InfluxDB v2.x
	flagToken        = "token" // #nosec
	flagOrganization = "organization"
	flagBucket       = "bucket"
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