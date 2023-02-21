package services

import (
	"context"
	"dl-shovel/kafka"
	"fmt"
	"github.com/Shopify/sarama"
)

type Interceptor func(ctx context.Context, message *sarama.ConsumerMessage) context.Context

type eventHandler struct {
	Service
}

func NewEventHandler(service Service) kafka.EventHandler {
	return &eventHandler{
		service,
	}
}

func (e *eventHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (e *eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (e *eventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Printf("Received key: %s, topic: %s \n", string(message.Key), message.Topic)

		err := e.Service.OperateEvent(context.Background(), message)
		if err != nil {
			fmt.Println("Error executing err: ", err)
		}

		session.MarkMessage(message, "")
	}

	return nil
}
