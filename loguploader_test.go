package ravenworker

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWrite(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
	}))
	defer ts.Close()

	uploader := &LogUploader{endpoint: ts.URL}

	body := "Log Upload"

	n, err := uploader.Write([]byte(body))
	if err != nil {
		log.Fatal(err)
	}

	if n != len(body) {
		log.Fatalf("Bad number of bytes written, want %d, got %d", len(body), n)
	}
}

func TestWriteError(t *testing.T) {

	uploader := &LogUploader{}

	body := "Log Upload"

	_, err := uploader.Write([]byte(body))

	t.Log(err)

	if err == nil {
		log.Fatal("expected an error got none.")
	}
}
