package client

import (
	"crypto/tls"
	"crypto/x509"
	"dl-shovel/configread"
	"dl-shovel/kafka"
	"dl-shovel/services"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"strings"
	"sync"
	"time"
)

type KafkaStretch interface {
	RunAllStretchShovels()
	runStretchedKafkaShovelListener(conf *configread.EnvConfig, shovel services.Shovel) chan bool
}

type kafkaStretch struct {
	kafkaStretchConfig *configread.EnvConfig
	kafkaSecret        *configread.KafkaStretchSecret
}

func NewKafkaStretch(kafkaStretchConfig *configread.EnvConfig, kafkaSecret *configread.KafkaStretchSecret) KafkaStretch {
	return &kafkaStretch{kafkaStretchConfig: kafkaStretchConfig, kafkaSecret: kafkaSecret}
}

func (k *kafkaStretch) RunAllStretchShovels() {
	var wg sync.WaitGroup
	var channels []chan bool
	shovels := k.getStretchedShovels(k.kafkaStretchConfig)
	for _, shovel := range shovels {
		wg.Add(1)
		channels = append(channels, k.runStretchedKafkaShovelListener(k.kafkaStretchConfig, shovel))
	}

	go func() {
		<-time.Tick(time.Duration(k.kafkaStretchConfig.RunningTime) * time.Minute)
		for _, v := range channels {
			wg.Done()
			close(v)
		}
	}()

	wg.Wait()
}

func (k *kafkaStretch) StretchedKafkaConfig(version string) *sarama.Config {
	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		panic(err)
	}
	//consumer
	config := sarama.NewConfig()
	config.Version = v
	config.Consumer.Return.Errors = true
	config.ClientID = "client-id"

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 5 * time.Second

	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
	config.Consumer.Fetch.Default = 2048 * 1024
	//producer
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 5 * time.Second
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Timeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.DialTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second
	config.Producer.MaxMessageBytes = 2000000

	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 5 * time.Second

	config.Net.SASL.Enable = true
	config.Net.SASL.User = k.kafkaSecret.KafkaUsername
	config.Net.SASL.Password = k.kafkaSecret.KafkaPassword
	config.Net.SASL.Handshake = true
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = createTLSConfiguration()

	return config
}

func (k *kafkaStretch) getStretchedShovels(conf *configread.EnvConfig) (result []services.Shovel) {
	config := k.StretchedKafkaConfig(conf.KafkaVersion)
	client, err := sarama.NewClient(conf.Brokers, config)
	if err != nil {
		panic(err)
	}
	defer func(client sarama.Client) {
		err := client.Close()
		if err != nil {
			fmt.Println(err)
			return
		}
	}(client)

	topics, err := client.Topics()
	if err != nil {
		panic(err)
	}

	for _, topic := range topics {
		for topicName, active := range conf.Topics {
			if active == false {
				continue
			}

			if !strings.Contains(topic, topicName) || !strings.Contains(topic, conf.DlPrefix) {
				continue
			}

			retryTopic := strings.ReplaceAll(topic, conf.DlPrefix, conf.RetryPrefix)
			result = append(result, services.Shovel{
				From: topic,
				To:   retryTopic,
			})
		}
	}
	return
}

func createTLSConfiguration() (t *tls.Config) {
	t = &tls.Config{}
	// you should add your certs to ./certs
	caCert, err := os.ReadFile("./certs/ca.pem")
	if err != nil {
		return nil
	}

	intCert, err := os.ReadFile("./certs/int.pem")
	if err != nil {
		return nil
	}

	intPkiCert, err := os.ReadFile("./certs/int-pki.pem")
	if err != nil {
		return nil
	}

	rootPkiCert, err := os.ReadFile("./certs/root-pki.pem")
	if err != nil {
		return nil
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	caCertPool.AppendCertsFromPEM(intCert)
	caCertPool.AppendCertsFromPEM(intPkiCert)
	caCertPool.AppendCertsFromPEM(rootPkiCert)

	t = &tls.Config{
		RootCAs: caCertPool,
	}

	return t
}

func (k *kafkaStretch) runStretchedKafkaShovelListener(conf *configread.EnvConfig, shovel services.Shovel) chan bool {
	notificationChannel := make(chan bool)
	config := kafka.ConnectionParameters{
		ConsumerGroupID: conf.GroupName,
		Conf:            k.StretchedKafkaConfig(conf.KafkaVersion),
		Brokers:         conf.Brokers,
		Topics:          []string{shovel.From},
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	producer, err := kafka.NewProducer(config)
	if err != nil {
		panic(err)
	}

	service := services.NewService(producer, shovel)
	handler := services.NewEventHandler(service)
	errChannel := consumer.Subscribe(handler)
	go func() {
		for e := range errChannel {
			fmt.Println(e)
			_ = producer.Close()
		}
	}()
	go func() {
		<-notificationChannel
		consumer.Unsubscribe()
	}()
	fmt.Printf("%v listener is starting \n", shovel.From)
	return notificationChannel
}
