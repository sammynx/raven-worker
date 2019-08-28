package ravenworker

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

var (
	TestProduceMessage = Message{
		MetaData: map[string]interface{}{
			"Test": "Test",
		},
		Content: Content("content"),
	}
)

// TestProduce will test the sequence of methods to be called during produce.
func TestProduce(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
		} else if r.URL.Path == "/flow/flowid/events" {
			if r.Method != http.MethodPost {
				t.Fatalf("Unexpected http method for /flow/flowid/events: %s", r.Method)
			}

			w.WriteHeader(http.StatusOK)

			w.Write([]byte("eventid"))
		} else if r.URL.Path == "/flow/flowid/events/eventid" {
			if r.Method != http.MethodPut {
				t.Fatalf("Unexpected http method for /flow/flowid/events: %s", r.Method)
			}

			w.WriteHeader(http.StatusOK)
		} else {
			t.Fatalf("Unexpected path requested: %s", r.URL.Path)
		}
	}))

	defer ts.Close()

	w, err := New(
		MustWithRavenURL(ts.URL),
		MustWithFlowID("flowid"),
		MustWithWorkerID("workerid"),
	)
	if err != nil {
		t.Fatalf("Could not initialize new raven worker: %s", err.Error())
	}

	if err := w.Produce(TestProduceMessage); err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}
}
