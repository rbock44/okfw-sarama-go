package sarama

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

//MessageProducer holds the kafka producer and some message counters
type MessageProducer struct {
	SuccessCount int64
	FailedCount  int64
	MessageCount int64
	Topic        string
	ClientID     string
	Producer     sarama.SyncProducer //sarama.AsyncProducer
}

func newMessageProducer(brokers []string, topic string, clientID string) (*MessageProducer, error) {
	kp := &MessageProducer{
		Topic:    topic,
		ClientID: clientID,
	}

	// For the access log, we are looking for AP semantics, with high throughput.
	// By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
	config := sarama.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V2_1_0_0

	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Wait for leader
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	config.Producer.Timeout = time.Second * 5
	config.Producer.Return.Successes = true
	// use idempotent requires these options
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("Failed to start Sarama producer: %s", err.Error())
	}

	kp.Producer = producer

	return kp, nil
}

//Close the producer
func (kp *MessageProducer) Close() {
	logger.Debugf("MessageProducer->Close\n")
	if kp.Producer != nil {
		kp.Producer.Close()
	}
}

//GetMessageCounter returns the address to the message counter
func (kp *MessageProducer) GetMessageCounter() *int64 {
	return &kp.MessageCount
}

//SendKeyValue send message with key and value
func (kp *MessageProducer) SendKeyValue(key []byte, value []byte) error {
	logger.Debugf("MessageProducer-SendKeyValue: send message [%#v] topic [%s]\n", key, kp.Topic)
	partition, offset, err := kp.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: kp.Topic,
		Key:   sarama.StringEncoder(string(key)),
		Value: sarama.StringEncoder(string(value)),
	})
	logger.Debugf("MessageProducer->SendKeyValue: partition [%d] offset [%d]\n", partition, offset)
	if err != nil {
		return err
	}
	return nil
}

//WaitUntilSendComplete wait until all messages are sent
func (kp *MessageProducer) WaitUntilSendComplete() {
	for kp.MessageCount > 0 {
		time.Sleep(time.Second * 1)
	}
}
