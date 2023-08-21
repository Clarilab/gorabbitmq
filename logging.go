package gorabbitmq

import (
	"log/slog"
	"os"
)

type log struct {
	logger []*slog.Logger
}

func newLogger(logger []*slog.Logger) *log {
	return &log{logger}
}

func (l *log) logDebug(msg string, args ...any) {
	for i := range l.logger {
		l.logger[i].Debug(msg, args...)
	}
}

func (l *log) logFatal(msg string, args ...any) {
	for i := range l.logger {
		l.logger[i].Error(msg, args...)
	}

	os.Exit(1) //nolint: revive // intended use of os.Exit
}

func (l *log) logError(msg string, args ...any) {
	for i := range l.logger {
		l.logger[i].Error(msg, args...)
	}
}

func (l *log) logInfo(msg string, args ...any) {
	for i := range l.logger {
		l.logger[i].Info(msg, args...)
	}
}

func (l *log) logWarn(msg string, args ...any) {
	for i := range l.logger {
		l.logger[i].Warn(msg, args...)
	}
}
