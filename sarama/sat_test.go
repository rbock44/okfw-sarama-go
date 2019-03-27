package sarama

import (
	"os"
	"testing"
	"time"

	"github.com/rbock44/okfw-kafka-go/kafka"
	zerolog "github.com/rbock44/okfw-zerolog-go/zerolog"
	"github.com/stretchr/testify/assert"
)

type testHandler struct {
	Validate func(context *kafka.MessageContext, key []byte, value []byte)
}

func (h *testHandler) Handle(context *kafka.MessageContext, key []byte, value []byte) {
	h.Validate(context, key, value)
}

func TestMessage_SendAndReadMessage(t *testing.T) {
	os.Setenv("LOG_LEVEL", "debug")
	SetLogger(zerolog.NewLogger())
	factory := NewFrameworkFactory([]string{"localhost:9092"})
	testTopic := "test_topic"
	testClientID := "sat_test"
	numberOfTestMessages := 10
	numberOfReceivedTestMessages := 0

	logger.Infof("consumer: create\n")
	consumer, err := factory.NewConsumer(testTopic, testClientID, &testHandler{
		Validate: func(context *kafka.MessageContext, key []byte, value []byte) {
			numberOfReceivedTestMessages++
			assert.Equal(t, []byte{0x0, 0x1, 0x2, 0x3, 0x4}, key)
			assert.Equal(t, []byte{0x5, 0x6, 0x7, 0x8}, value)
		},
	})
	if err != nil {
		t.Error(t, err, "cannot create consumer")
		return
	}

	defer consumer.Close()

	logger.Infof("consumer: successfully created\n")

	producer, err := factory.NewProducer(testTopic, testClientID)
	if err != nil {
		t.Error(t, err, "cannot create producer")
		return
	}
	defer producer.Close()

	logger.Infof("producer: created\n")

	for i := 0; i < numberOfTestMessages; i++ {
		err = producer.SendKeyValue([]byte{0, 1, 2, 3, 4}, []byte{5, 6, 7, 8})
		if err != nil {
			t.Error(t, err, "cannot send key value")
			return
		}
	}

	for i := 0; i < 10 && *consumer.GetMessageCounter() != 10; i++ {
		err := consumer.Process(1000)
		assert.Nil(t, err)
		logger.Infof("wait for messages to arrive i [%d]\n", i)
		time.Sleep(time.Second * 1)
	}
	assert.Equal(t, int64(numberOfTestMessages), *consumer.GetMessageCounter())
}
