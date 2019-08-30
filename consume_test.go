package ravenworker

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	uuid "github.com/satori/go.uuid"
)

// TODO: remove backoff in case of test
func TestConsume(t *testing.T) {
	ackID := uuid.NewV4()
	eventID := uuid.NewV4()

	srvr, err := testServer(&workflowServer{
		getJob: func(getJob Workflow_getJob) error {
			getJob.Results.SetAckID(ackID.Bytes())
			getJob.Results.SetEventID(eventID.Bytes())
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Could not start test server: %s", err.Error())
	}

	defer srvr.Close()

	w, err := New(
		MustWithRavenURL(fmt.Sprintf("capnproto://%s", srvr.Addr().String())),
		MustWithFlowID(flowID.String()),
		MustWithWorkerID(workerID.String()),
		// TODO: WithMaxRetries(0),
	)
	if err != nil {
		t.Fatalf("Could not initialize new raven worker: %s", err.Error())
	}

	ref, err := w.Consume()
	if err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	if ref.AckID != ackID.String() {
		t.Errorf("AckID isn't right. Got %s, want %s", ref.AckID, ackID.String())
	}

	if ref.EventID != eventID.String() {
		t.Errorf("EventID isn't right. Got %s, want %s", ref.EventID, eventID.String())
	}
}

// TODO: remove backoff in case of test
func TestConsumeAck(t *testing.T) {
	ackID := uuid.NewV4()
	eventID := uuid.NewV4()

	srvr, err := testServer(&workflowServer{
		getJob: func(getJob Workflow_getJob) error {
			getJob.Results.SetAckID(ackID.Bytes())
			getJob.Results.SetEventID(eventID.Bytes())
			return nil
		},
		ackJob: func(ackJob Workflow_ackJob) error {
			// TODO: verify input params
			ackJob.Results.SetAcked(true)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Could not start test server: %s", err.Error())
	}

	defer srvr.Close()

	w, err := New(
		MustWithRavenURL(fmt.Sprintf("capnproto://%s", srvr.Addr().String())),
		MustWithFlowID(flowID.String()),
		MustWithWorkerID(workerID.String()),
		// TODO: WithMaxRetries(0),
	)
	if err != nil {
		t.Fatalf("Could not initialize new raven worker: %s", err.Error())
	}

	ref, err := w.Consume()
	if err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	if err := w.Ack(ref); err != nil {
		t.Errorf("Ack failed: %s", err.Error())
	}
}

// TODO: remove backoff in case of test
func TestConsumeAckWithContent(t *testing.T) {
	ackID := uuid.NewV4()
	eventID := uuid.NewV4()

	msg := Message{
		Content: JsonContent("test"),
	}

	srvr, err := testServer(&workflowServer{
		getJob: func(getJob Workflow_getJob) error {
			getJob.Results.SetAckID(ackID.Bytes())
			getJob.Results.SetEventID(eventID.Bytes())
			return nil
		},
		ackJob: func(ackJob Workflow_ackJob) error {
			event, err := ackJob.Params.Event()
			if err != nil {
				return err
			}

			if v, err := event.Content(); err != nil {
				return err
			} else if !bytes.Equal(JsonContent("test"), v) {
				return fmt.Errorf("Incorrect message content")
			}

			ackJob.Results.SetAcked(true)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Could not start test server: %s", err.Error())
	}

	defer srvr.Close()

	w, err := New(
		MustWithRavenURL(fmt.Sprintf("capnproto://%s", srvr.Addr().String())),
		MustWithFlowID(flowID.String()),
		MustWithWorkerID(workerID.String()),
		// TODO: WithMaxRetries(0),
	)
	if err != nil {
		t.Fatalf("Could not initialize new raven worker: %s", err.Error())
	}

	ref, err := w.Consume()
	if err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	if err := w.Ack(ref, WithMessage(msg)); err != nil {
		t.Errorf("Ack failed: %s", err.Error())
	}
}

func TestConsumeGet(t *testing.T) {
	ackID := uuid.NewV4()
	eventID := uuid.NewV4()

	srvr, err := testServer(&workflowServer{
		getJob: func(getJob Workflow_getJob) error {
			getJob.Results.SetAckID(ackID.Bytes())
			getJob.Results.SetEventID(eventID.Bytes())

			return nil
		},
		getEvent: func(getEvent Workflow_getEvent) error {
			evt, err := getEvent.Results.NewEvent()
			if err != nil {
				return err
			}

			evt.SetContent(JsonContent("test"))
			getEvent.Results.SetEvent(evt)

			return nil
		},
	})
	if err != nil {
		t.Fatalf("Could not start test server: %s", err.Error())
	}

	defer srvr.Close()

	w, err := New(
		MustWithRavenURL(fmt.Sprintf("capnproto://%s", srvr.Addr().String())),
		MustWithFlowID(flowID.String()),
		MustWithWorkerID(workerID.String()),
		// TODO: WithMaxRetries(0),
	)
	if err != nil {
		t.Fatalf("Could not initialize new raven worker: %s", err.Error())
	}

	ref, err := w.Consume()
	if err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	message, err := w.Get(ref)
	if err != nil {
		t.Errorf("Get failed: %s", err.Error())
	}

	if diff := cmp.Diff(message, Message{
		MetaData: []Metadata{},
		Content:  JsonContent("test"),
	}); diff != "" {
		t.Fatalf("Get() mismatch (-want +got):\n%s", diff)
	}
}
