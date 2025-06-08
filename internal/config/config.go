package config

import (
	"log/slog"

	"github.com/spf13/viper"
)

type MeteorDbConfig struct {
	// Server Configuration
	Host     string `mapstructure:"host" default:"0.0.0.0" description:"the sql host address"`
	Port     string `mapstructure:"port" default:"7653" description:"the sql read port"`
	LogLevel string `mapstructure:"logLevel" default:"info" description:"Log Level"`
	UseWal   bool   `mapstructure:"useWal" default:"true" description:"Whether to use write ahead log"`
}

var Config *MeteorDbConfig
const configPath = "./"

func LoadConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(configPath)

	if err := viper.ReadInConfig(); err != nil {
		slog.Error("Failed to read config")
		panic(err)
	}

	if err := viper.Unmarshal(&Config); err != nil {
		slog.Error("Failed to parse config")
		panic(err)
	}
}
