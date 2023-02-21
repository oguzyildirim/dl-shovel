package services

import (
	"context"
	"github.com/Shopify/sarama"
)

type Shovel struct {
	From string
	To   string
}

type service struct {
	producer sarama.SyncProducer
	shovel   Shovel
}

type Service interface {
	OperateEvent(ctx context.Context, message *sarama.ConsumerMessage) error
}

func NewService(producer sarama.SyncProducer, shovel Shovel) Service {
	return &service{
		shovel:   shovel,
		producer: producer,
	}
}

func (s *service) OperateEvent(ctx context.Context, message *sarama.ConsumerMessage) error {

	_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic:   s.shovel.To,
		Value:   sarama.StringEncoder(message.Value),
		Headers: getHeaders(message),
		Key:     sarama.StringEncoder(message.Key),
	})

	return err
}

func getHeaders(message *sarama.ConsumerMessage) []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0)
	for _, header := range message.Headers {
		headers = append(headers, *header)
	}
	return headers
}
