package configread

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"strings"
	"sync"
	"time"
)

type ConfigurationManager interface {
	GetKafkaStretchSecret() *KafkaStretchSecret
	GetStretchedKafkaConfig() *EnvConfig
}

type configurationManager struct {
	configuration *ApplicationConfig
}

func (c configurationManager) GetStretchedKafkaConfig() *EnvConfig {
	return &c.configuration.StretchedKafka
}

func (c configurationManager) GetKafkaStretchSecret() *KafkaStretchSecret {
	return &c.configuration.KafkaStretchSecret
}

const (
	configurationFilePath = "./configs/env.yaml"
	secretFilePath        = "./configs/secret.yaml"
)

func NewConfigurationManager() ConfigurationManager {
	configuration := new(ApplicationConfig)

	prepareConfiguration(configuration)
	return &configurationManager{
		configuration: configuration,
	}
}

func prepareConfiguration(configuration *ApplicationConfig) {
	viperConfigReader := viper.New()
	viperSecretReader := viper.New()
	viperConfigReader.SetConfigName("local")
	viperConfigReader.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viperConfigReader.SetConfigType("yaml")

	loadConfiguration(viperConfigReader, configuration, configurationFilePath)
	loadSecret(viperSecretReader, configuration, secretFilePath)

	viperConfigReader.WatchConfig()
	viperConfigReader.OnConfigChange(func(e fsnotify.Event) {
		loadConfiguration(viperConfigReader, configuration, configurationFilePath)
	})

	viperSecretReader.WatchConfig()
	viperSecretReader.OnConfigChange(func(e fsnotify.Event) {
		loadSecret(viperSecretReader, configuration, secretFilePath)
	})
	go func() {
		for range time.NewTicker(1 * time.Minute).C {
			loadConfiguration(viperConfigReader, configuration, configurationFilePath)
			loadSecret(viperSecretReader, configuration, secretFilePath)
		}
	}()

}

func loadConfiguration(viperInstance *viper.Viper, configuration *ApplicationConfig, configurationFilePath string) {
	configMutex := &sync.Mutex{}
	viperInstance.SetConfigFile(configurationFilePath)
	configurationError := viperInstance.ReadInConfig()
	ErrorNotNil(configurationError, "cannot read config for file path : "+configurationFilePath)
	configMutex.Lock()
	err := viperInstance.Unmarshal(&configuration)
	ErrorNotNil(err, "cannot initialize configurations for file path"+configurationFilePath)
	configMutex.Unlock()
}

func loadSecret(viperInstance *viper.Viper, configuration *ApplicationConfig, configurationFilePath string) {
	configMutex := &sync.Mutex{}
	viperInstance.SetConfigFile(configurationFilePath)
	configurationError := viperInstance.ReadInConfig()
	ErrorNotNil(configurationError, "cannot read config for file path : "+configurationFilePath)
	configMutex.Lock()
	configuration.KafkaStretchSecret.KafkaUsername = viperInstance.GetString("kafkaUsername")
	configuration.KafkaStretchSecret.KafkaPassword = viperInstance.GetString("kafkaPassword")
	configMutex.Unlock()
}

func ErrorNotNil(err error, message string) {
	if err != nil {
		panic(fmt.Sprintf("%s, error : %s", message, err.Error()))
	}
}
