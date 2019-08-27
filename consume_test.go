package ravenworker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var (
	TestConsumeMessage = map[string]interface{}{
		"metadata": map[string]interface{}{
			"Test": "Test",
		},
		"content": "content",
	}
)

// TODO: change this to use mux
// and have mux.Handle() for paths
func DefaultConsumeHandler(t *testing.T, w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
	} else if r.URL.Path == "/workers/workerid/work" {
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(map[string]interface{}{
			"ack_id":   "ackid",
			"event_id": "eventid",
			"flow_id":  "flowid",
		}); err != nil {
			t.Fatalf("Could not write work response: %v", err)
		}
	} else if r.URL.Path == "/flow/flowid/events/eventid" {
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(TestConsumeMessage); err != nil {
			t.Fatalf("Could not write event response: %v", err)
		}
	} else if r.URL.Path == "/workers/workerid/ack/ackid" {
		ar := ackRequest{}
		if err := json.NewDecoder(r.Body).Decode(&ar); err != nil {
			t.Fatalf("Could not parse ack request body: %v", err)
		}

		if diff := cmp.Diff(ackRequest{
			Content: "content",
			Metadata: map[string]interface{}{
				"Test": "Test",
			},
			Filter: false,
		}, ar); diff != "" {
			t.Fatalf("Ack() request body mismatch (-want +got):\n%s", diff)
		}
	} else {
		t.Fatalf("Unexpected path requested: %s", r.URL.Path)
	}
}

// TestConsumeAck will test the sequences of retrieving work, retrieving the event and ack'ing the event.
func TestConsumeAck(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		DefaultConsumeHandler(t, w, r)
	}))

	defer ts.Close()

	fmt.Println(ts.URL)
	w, err := New(
		MustWithRavenURL(ts.URL),
		MustWithFlowID("flowid"),
		MustWithWorkerID("workerid"),
	)
	if err != nil {
		t.Fatalf("Could not initialize new raven worker: %s", err.Error())
	}

	message, err := w.Consume()
	if err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	want := TestConsumeMessage
	got := map[string]interface{}{
		"metadata": message.metaData,
		"content":  string(message.content),
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Consume() mismatch (-want +got):\n%s", diff)
	}

	if err := w.Ack(message); err != nil {
		t.Fatalf("Could not ack message: %s", err.Error())
	}
}

// TestConsumeAck will test the sequences of retrieving work, retrieving the event and ack'ing the event.
func TestConsumeAckWithUpdatedContent(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/workers/workerid/ack/ackid" {
			ar := ackRequest{}
			if err := json.NewDecoder(r.Body).Decode(&ar); err != nil {
				t.Fatalf("Could not parse ack request body: %v", err)
			}

			if diff := cmp.Diff(ackRequest{
				Content: "updated content",
				Metadata: map[string]interface{}{
					"Test": "Test",
				},
				Filter: false,
			}, ar); diff != "" {
				t.Fatalf("Ack() request body mismatch (-want +got):\n%s", diff)
			}
		} else {
			DefaultConsumeHandler(t, w, r)
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

	message, err := w.Consume()
	if err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	want := TestConsumeMessage
	got := map[string]interface{}{
		"metadata": message.metaData,
		"content":  string(message.content),
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Consume() mismatch (-want +got):\n%s", diff)
	}

	message = message.Content([]byte("updated content"))

	if err := w.Ack(message); err != nil {
		t.Fatalf("Could not ack message: %s", err.Error())
	}
}

// TestConsumeAck will test the sequences of retrieving work, retrieving the event and ack'ing the event.
func TestConsumeAckWithFilter(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/workers/workerid/ack/ackid" {
			ar := ackRequest{}
			if err := json.NewDecoder(r.Body).Decode(&ar); err != nil {
				t.Fatalf("Could not parse ack request body: %v", err)
			}

			if diff := cmp.Diff(ackRequest{
				Content: "content",
				Metadata: map[string]interface{}{
					"Test": "Test",
				},
				Filter: true,
			}, ar); diff != "" {
				t.Fatalf("Ack() request body mismatch (-want +got):\n%s", diff)
			}
		} else {
			DefaultConsumeHandler(t, w, r)
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

	message, err := w.Consume()
	if err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	want := TestConsumeMessage
	got := map[string]interface{}{
		"metadata": message.metaData,
		"content":  string(message.content),
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Consume() mismatch (-want +got):\n%s", diff)
	}

	if err := w.Ack(message, WithFilter()); err != nil {
		t.Fatalf("Could not ack message: %s", err.Error())
	}
}
