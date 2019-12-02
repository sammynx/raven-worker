package ravenworker

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"

	"gitlab.com/z0mbie42/rz-go/v2"
)

func TestNewDefaultLogger(t *testing.T) {

	deflog := NewDefaultLogger("test", "123")

	if deflog == nil {
		t.Fatal("could not initialize defaultlogger. returned <nil>")
	}

	if deflog.upload == nil {
		t.Fatal("could not initialize logUploader. returned <nil>")
	}
}

func TestLevelInfo(t *testing.T) {
	buf := &bytes.Buffer{}

	l := &defaultLogger{Logger: rz.New(rz.Writer(buf))}

	format := "A%s"
	arg := "B"
	want := "\"AB\""

	l.Infof(format, arg)

	if !strings.Contains(buf.String(), want) {
		t.Fatalf("did not find message, %s, in output: %s", want, buf.String())
	}
}

func TestLevelDebug(t *testing.T) {
	buf := &bytes.Buffer{}

	l := &defaultLogger{Logger: rz.New(rz.Writer(buf))}

	format := "A%s"
	arg := "B"
	want := "\"AB\""

	l.Debugf(format, arg)

	if !strings.Contains(buf.String(), want) {
		t.Fatalf("did not find message, %s, in output: %s", want, buf.String())
	}
}

func TestLevelError(t *testing.T) {
	buf := &bytes.Buffer{}

	l := &defaultLogger{Logger: rz.New(rz.Writer(buf))}

	format := "A%s"
	arg := "B"
	want := "\"AB\""

	l.Errorf(format, arg)

	if !strings.Contains(buf.String(), want) {
		t.Fatalf("did not find message, %s, in output: %s", want, buf.String())
	}
}

func TestLevelFatal(t *testing.T) {
	buf := &bytes.Buffer{}

	l := &defaultLogger{Logger: rz.New(rz.Writer(buf))}

	format := "A%s"
	arg := "B"
	want := "\"AB\""

	if os.Getenv("LOG_FATAL") == "1" {
		l.Fatalf(format, arg)

		if !strings.Contains(buf.String(), want) {
			t.Fatalf("did not find message, %s, in output: %s", want, buf.String())
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestLevelFatal")
	cmd.Env = append(os.Environ(), "LOG_FATAL=1")
	err := cmd.Run()

	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}
