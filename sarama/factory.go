package sarama

import (
	"fmt"

	"github.com/rbock44/okfw-kafka-go/kafka"
)

//FrameworkFactory creates consumer and provider for the okfw-kafka-go
type FrameworkFactory struct {
	Brokers []string
}

//NewConsumer creaes a new confluent consumer
func (f *FrameworkFactory) NewConsumer(topic string, clientID string, handler kafka.MessageHandler) (kafka.MessageConsumer, error) {
	return newMessageConsumer(f.Brokers, topic, clientID, handler)
}

//NewProducer creates a new confluent provider
func (f *FrameworkFactory) NewProducer(topic string, clientID string) (kafka.MessageProducer, error) {
	return newMessageProducer(f.Brokers, topic, clientID)
}

//NewSchemaResolver creates a new registry
func (f *FrameworkFactory) NewSchemaResolver() (kafka.SchemaResolver, error) {
	_, err := getKafkaSchemaClient().Subjects()
	if err != nil {
		return nil, fmt.Errorf("cannot query subjects on kafka registry [%s]", err.Error())
	}
	return newSchemaResolver(), nil
}

//NewFrameworkFactory creates the consumer and provider factory
func NewFrameworkFactory(brokers []string) *FrameworkFactory {
	return &FrameworkFactory{Brokers: brokers}
}
