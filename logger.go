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

var DefaultLogger = NewDefaultLogger(os.Getenv("RAVEN_LOG"))

type defaultLogger struct {
	rz.Logger

	upload *logUploader // need this for closing the logger.
}

//NewDefaultLogger creates a JSON logger which outputs to an http endpoint.
// provide an empty string as endpoint to log to stdout.
func NewDefaultLogger(endpoint string) *defaultLogger {

	if endpoint == "" {
		return &defaultLogger{
			Logger: rz.New(
				rz.Fields(rz.Timestamp(true)),
				rz.Writer(os.Stdout),
			),
		}
	}

	l := NewlogUploader(context.Background(), endpoint)

	return &defaultLogger{
		Logger: rz.New(
			rz.Fields(rz.Timestamp(true)),
			rz.Writer(l),
		),
		upload: l,
	}
}

func (l *defaultLogger) Close() error {
	return l.upload.Close()
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
