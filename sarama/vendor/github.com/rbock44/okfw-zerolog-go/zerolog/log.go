package log

import (
	"os"
	"strings"

	logapi "github.com/rbock44/okfw-logapi-go/logapi"
	"github.com/rs/zerolog"
)

//LoggerImpl logger implementation
type LoggerImpl struct {
	Logger     zerolog.Logger
	InfoLevel  bool
	DebugLevel bool
}

//NewLogger creates a new logger
func NewLogger() logapi.Logger {
	logLevel := strings.ToLower(os.Getenv("LOG_LEVEL"))

	l := &LoggerImpl{}
	if logLevel == "info" {
		l.InfoLevel = true
	}
	if logLevel == "debug" {
		l.InfoLevel = true
		l.DebugLevel = true
	}

	l.Logger = zerolog.New(os.Stdout)
	return l
}

//IsLevelInfo returns true if log level is info
func (l *LoggerImpl) IsLevelInfo() bool {
	return l.InfoLevel
}

//IsLevelDebug returns true if log level is debug
func (l *LoggerImpl) IsLevelDebug() bool {
	return l.DebugLevel
}

//Infof log with info level
func (l *LoggerImpl) Infof(format string, args ...interface{}) {
	if l.InfoLevel {
		l.Logger.Info().Msgf(format, args...)
	}
}

//Debugf log with debug level
func (l *LoggerImpl) Debugf(format string, args ...interface{}) {
	if l.DebugLevel {
		l.Logger.Debug().Msgf(format, args...)
	}
}

//Warnf log with warn level
func (l *LoggerImpl) Warnf(format string, args ...interface{}) {
	l.Logger.Warn().Msgf(format, args...)
}

//Errorf log error
func (l *LoggerImpl) Errorf(format string, args ...interface{}) {
	l.Logger.Error().Msgf(format, args...)
}

//Fatalf log and abort
func (l *LoggerImpl) Fatalf(format string, args ...interface{}) {
	l.Logger.Fatal().Msgf(format, args...)
}
