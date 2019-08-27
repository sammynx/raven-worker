package ravenworker

import "fmt"

type Logger interface {
	Debugf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
	Fatalf(msg string, args ...interface{})
}

var DefaultLogger Logger = &defaultLogger{}

type defaultLogger struct {
}

// TODO: improving logging
func (l *defaultLogger) Infof(msg string, args ...interface{}) {
	fmt.Printf(msg, args...)
	fmt.Println("")
}

// TODO: improving logging
func (l *defaultLogger) Debugf(msg string, args ...interface{}) {
	fmt.Printf(msg, args...)
	fmt.Println("")
}

// TODO: improving logging
func (l *defaultLogger) Errorf(msg string, args ...interface{}) {
	fmt.Printf(msg, args...)
	fmt.Println("")
}

func (l *defaultLogger) Fatalf(msg string, args ...interface{}) {
	panic(fmt.Sprintf(msg, args...))
}
