package main

import (
	"dl-shovel/configread"
	"dl-shovel/kafka/client"
	"fmt"
)

func main() {
	manager := configread.NewConfigurationManager()

	kafkaStretchConfig := manager.GetStretchedKafkaConfig()
	kafkaStretchSecret := manager.GetKafkaStretchSecret()

	kafkaStretch := client.NewKafkaStretch(kafkaStretchConfig, kafkaStretchSecret)

	kafkaStretch.RunAllStretchShovels()
	fmt.Println("completed")
}
