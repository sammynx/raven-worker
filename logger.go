package ravenworker

import (
	"fmt"
	"os"

	"gitlab.com/z0mbie42/rz-go/v2"
	context "golang.org/x/net/context"
)

type Logger interface {
	Debugf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
	Fatalf(msg string, args ...interface{})
}

var DefaultLogger = NewDefaultLogger(os.Getenv("RAVEN_LOG"), os.Getenv("WORKER_ID"))

type defaultLogger struct {
	rz.Logger

	LogCloser // need this for closing the logger.
}

type LogCloser interface {
	Close() error
}

type NullLogCloser struct {
}

func (NullLogCloser) Close() error {
	return nil
}

//NewDefaultLogger creates a JSON logger which outputs to an http endpoint.
//provide an empty string as endpoint to log to stdout.
func NewDefaultLogger(endpoint, id string) *defaultLogger {
	// TODO: add flow_id, worker_id and block_id
	logger := rz.New(
		rz.Fields(rz.Timestamp(true), rz.String("worker-id", id)),
	)

	var logCloser LogCloser = &NullLogCloser{}

	lvl := rz.InfoLevel

	if s := os.Getenv("LOG_LEVEL"); s == "" {
	} else if parsedLevel, err := rz.ParseLevel(s); err != nil {
		logger.Fatal(fmt.Sprintf("Could not parse log level: %s", s))
	} else {
		lvl = parsedLevel
	}

	// always write to stdout
	logger = logger.With(rz.Level(lvl), rz.Writer(rz.SyncWriter(os.Stdout)), rz.Formatter(rz.FormatterConsole()))

	if endpoint == "" {
	} else if l, err := NewLogUploader(context.Background(), endpoint); err != nil {
		logger.Fatal(fmt.Sprintf("Could not configure log uploader: %s", err))
	} else {
		logCloser = l
		logger = logger.With(rz.Level(lvl), rz.Writer(l))
	}

	return &defaultLogger{
		Logger:    logger,
		LogCloser: logCloser,
	}
}

// TODO: improving logging
func (l *defaultLogger) Infof(msg string, args ...interface{}) {
	l.Info(fmt.Sprintf(msg, args...))
}

// TODO: improving logging
func (l *defaultLogger) Debugf(msg string, args ...interface{}) {
	l.Debug(fmt.Sprintf(msg, args...))
}

// TODO: improving logging
func (l *defaultLogger) Errorf(msg string, args ...interface{}) {
	l.Error(fmt.Sprintf(msg, args...))
}

func (l *defaultLogger) Fatalf(msg string, args ...interface{}) {
	//TODO (jerry 2019-10-23): Add stacktrace
	l.Fatal(fmt.Sprintf(msg, args...))
}
