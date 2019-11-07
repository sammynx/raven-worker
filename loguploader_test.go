package ravenworker

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	context "golang.org/x/net/context"
)

func TestNewClosePanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("got panic, %v", r)
		}
	}()

	l := NewlogUploader(context.Background(), "")
	l.Close()
}

func TestNewlogUploader(t *testing.T) {
	tt := NewlogUploader(context.Background(), "test")

	if tt.endpoint != "test" {
		t.Errorf("endpoint not set: expected 'test', got '%s'", tt.endpoint)
	}

	if tt.cancel == nil {
		t.Fatalf("cancel function not set")
	}
}

func TestWrite(t *testing.T) {
	l := &logUploader{}

	tt := "test"

	n, _ := l.Write([]byte(tt))

	if n != len(tt) {
		t.Fatalf("bad lenght of Write, want %d, got %d", len(tt), n)
	}

	if l.buf.String() != tt {
		t.Fatalf("buffer write error, want %s, got %s", tt, l.buf.String())
	}
}

func TestSend(t *testing.T) {
	buf := &bytes.Buffer{}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(buf, r.Body)
	}))
	defer ts.Close()

	tt := NewlogUploader(context.Background(), ts.URL)

	body := "uploaded"

	_, err := tt.buf.Write([]byte(body))
	if err != nil {
		log.Fatal(err)
	}

	tt.Close()

	if buf.String() != body {
		t.Fatalf("did not send body. expected %s, got %s", body, buf.String())
	}
}
