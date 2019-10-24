package ravenworker

import (
	"fmt"
	"io"
	"os"

	"gitlab.com/z0mbie42/rz-go/v2"
)

type Logger interface {
	Debugf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
	Fatalf(msg string, args ...interface{})
}

var DefaultLogger Logger = NewDefaultLogger(os.Getenv("RAVEN_LOG"))

type defaultLogger struct {
	rz.Logger
}

const loggerOK = "{\"logger ok.\"}"

//NewDefaultLogger creates a JSON logger which outputs to a Raven Fluentd endpoint.
// If endpoint is not available it defaults to Stdout.
func NewDefaultLogger(endpoint string) *defaultLogger {
	var w io.Writer = os.Stdout

	// check connection.
	l := &LogUploader{endpoint: endpoint}
	_, err := l.Write([]byte(loggerOK))
	if err == nil {
		w = l
	}

	// make this threadfsafe with a syncwriter.
	writer := rz.SyncWriter(w)

	logger := rz.New(
		rz.Fields(rz.Timestamp(true)),
		rz.Writer(writer),
	)

	return &defaultLogger{Logger: logger}
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
