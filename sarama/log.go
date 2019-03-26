package sarama

import (
	ss "github.com/Shopify/sarama"
	"github.com/rbock44/okfw-logapi-go/logapi"
)

//Logger logger implementation
var logger logapi.Logger

type saramaLogger struct{}

func (l saramaLogger) Print(args ...interface{}) {
	logger.Debugf("#v", args)
}

func (l saramaLogger) Printf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

func (l saramaLogger) Println(args ...interface{}) {
	logger.Debugf("#v", args)
}

func init() {
	ss.Logger = saramaLogger{}
}
