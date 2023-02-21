package configread

type ApplicationConfig struct {
	KafkaStretchSecret KafkaStretchSecret
	StretchedKafka     EnvConfig
}

type EnvConfig struct {
	Topics       map[string]bool
	RetryPrefix  string
	DlPrefix     string
	Brokers      []string
	KafkaVersion string
	RunningTime  int
	GroupName    string
}

type KafkaStretchSecret struct {
	KafkaUsername string
	KafkaPassword string
}
