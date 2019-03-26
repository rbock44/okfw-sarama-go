package sarama

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	ss "github.com/Shopify/sarama"
	"github.com/rbock44/okfw-kafka-go/kafka"
)

//MessageConsumer high level consumer wrapper
type MessageConsumer struct {
	Topic          string
	ClientID       string
	Consumer       sarama.ConsumerGroup
	Handler        *saramaHandler
	FailedCount    int64
	IgnoredCount   int64
	DeliveredCount int64
}

type saramaHandler struct {
	ready          chan bool
	MessageHandler kafka.MessageHandler
	Messages       []*sarama.ConsumerMessage
	DeliveredCount *int64
	FailedCount    *int64
}

func newMessageConsumer(brokers []string, topic string, clientID string, messageHandler kafka.MessageHandler) (kafka.MessageConsumer, error) {
	config := ss.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V2_1_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerGroup, err := ss.NewConsumerGroup(brokers, clientID, config)
	if err != nil {
		return nil, err
	}

	kc := &MessageConsumer{
		Topic:    topic,
		ClientID: clientID,
		Consumer: consumerGroup,
	}

	kc.Consumer = consumerGroup
	kc.Handler = &saramaHandler{
		ready:          make(chan bool, 0),
		MessageHandler: messageHandler,
		DeliveredCount: &kc.DeliveredCount,
		FailedCount:    &kc.FailedCount,
	}

	return kc, nil
}

//SetHandler sets the message handler
func (kc *MessageConsumer) SetHandler(handler kafka.MessageHandler) {
	kc.Handler.MessageHandler = handler
}

//Process read messages and delete to handler
func (kc *MessageConsumer) Process(timeoutMs int) error {
	logger.Debugf("MessageConsumer->Process [%d]\n", timeoutMs)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()
	err := kc.Consumer.Consume(ctx, []string{kc.Topic}, kc.Handler)
	if err != nil {
		return fmt.Errorf("process error [%s]", err.Error())
	}

	if ctx.Err() != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil
		}
		return ctx.Err()
	}

	return nil
}

//GetBacklog read an event in case no event is available return nil
func (kc *MessageConsumer) GetBacklog() (int, error) {
	return 0, nil
}

//GetMessageCounter get the message counter
func (kc *MessageConsumer) GetMessageCounter() *int64 {
	return &kc.DeliveredCount
}

//Close close the consumer
func (kc *MessageConsumer) Close() {
	kc.Consumer.Close()
}

//Implement sarama consumer group handler

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *saramaHandler) Setup(sarama.ConsumerGroupSession) error {
	logger.Debugf("saramaHandler->Setup\n")
	// Mark the consumer as ready
	//close(h.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *saramaHandler) Cleanup(sarama.ConsumerGroupSession) error {
	logger.Debugf("saramaHandler->Cleanup\n")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *saramaHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	logger.Debugf("saramaHandler->ConsumeClaim\n")
	for message := range claim.Messages() {
		logger.Debugf("saramaHandler->ConsumeClaim: call message handler\n")
		h.MessageHandler.Handle(&kafka.MessageContext{
			Timestamp: message.Timestamp,
		}, message.Key, message.Value)
		logger.Debugf("saramaHandler->ConsumeClaim: increment delivered counter [%d]\n", *h.DeliveredCount)
		*h.DeliveredCount++
		session.MarkMessage(message, "")
	}
	return nil
}
