package ravenworker

import "testing"

func TestWithLogger(t *testing.T) {
	tf, _ := WithLogger(&defaultLogger{})

	if tf == nil {
		t.Fatal("logger not set: OptionFunc is <nil>")
	}

	c := &Config{}

	tf(c)

	if c.log == nil {
		t.Fatal("logger not set: Config.log is <nil>")
	}

	// defaultlogger implements io.Closer.
	if len(c.closers) == 0 {
		t.Fatal("no Closer added to Config.closers")
	}
}

func TestWithLoggerError(t *testing.T) {
	_, err := WithLogger(nil)

	if err == nil {
		t.Fatal("expected an error: got none")
	}
}
