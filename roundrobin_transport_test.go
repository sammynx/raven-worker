package ravenworker

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestRoundTripper will test the sequences of retrieving work, retrieving the event and ack'ing the event.
func TestRoundTripper(t *testing.T) {
	count := 0

	urls := make([]string, 5)

	for i, _ := range urls {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count++

			DefaultConsumeHandler(t, w, r)
		}))

		// defer will be called on exit of function, not on exit of loop
		defer ts.Close()

		urls[i] = ts.URL
	}

	w, err := New(
		MustWithRavenURL(strings.Join(urls, ",")),
		MustWithFlowID("flowid"),
		MustWithWorkerID("workerid"),
	)
	if err != nil {
		t.Fatalf("Could not initialize new raven worker: %s", err.Error())
	}

	for i := 0; i < 100; i++ {
		_, err := w.Consume()
		if err != nil {
			t.Fatalf("Could not consume message: %s", err.Error())
		}
	}

	if count != 200 {
		t.Fatalf("Expected server picks to be 100: %d", count)
	}
}
