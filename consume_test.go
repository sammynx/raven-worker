package ravenworker

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/cenkalti/backoff"
	"github.com/dutchsec/raven-worker/workflow"
	"github.com/google/go-cmp/cmp"
	uuid "github.com/satori/go.uuid"
)

func TestConsume(t *testing.T) {
	ackID := uuid.NewV4()
	eventID := uuid.NewV4()

	srvr, err := testServer(&workflowServer{
		getJob: func(getJob workflow.Workflow_getJob) error {
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
		WithBackOff(StopBackOff),
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

func TestConsumeAck(t *testing.T) {
	ackID := uuid.NewV4()
	eventID := uuid.NewV4()

	srvr, err := testServer(&workflowServer{
		getJob: func(getJob workflow.Workflow_getJob) error {
			getJob.Results.SetAckID(ackID.Bytes())
			getJob.Results.SetEventID(eventID.Bytes())
			return nil
		},
		ackJob: func(ackJob workflow.Workflow_ackJob) error {
			if v, err := ackJob.Params.AckID(); err != nil {
				return err
			} else if id, err := uuid.FromBytes(v); err != nil {
				return fmt.Errorf("AckID is not a valid UUID: %s", err)
			} else if !uuid.Equal(id, ackID) {
				return fmt.Errorf("Unexpected AckID: got=%s", v)
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
		WithBackOff(StopBackOff),
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

func TestConsumeAckWithContent(t *testing.T) {
	ackID := uuid.NewV4()
	eventID := uuid.NewV4()

	msg := Message{
		Content: JsonContent("test"),
	}

	srvr, err := testServer(&workflowServer{
		getJob: func(getJob workflow.Workflow_getJob) error {
			getJob.Results.SetAckID(ackID.Bytes())
			getJob.Results.SetEventID(eventID.Bytes())
			return nil
		},
		ackJob: func(ackJob workflow.Workflow_ackJob) error {
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
		WithBackOff(StopBackOff),
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
		getJob: func(getJob workflow.Workflow_getJob) error {
			getJob.Results.SetAckID(ackID.Bytes())
			getJob.Results.SetEventID(eventID.Bytes())

			return nil
		},
		getEvent: func(getEvent workflow.Workflow_getEvent) error {
			if v, err := getEvent.Params.EventID(); err != nil {
				return err
			} else if id, err := uuid.FromBytes(v); err != nil {
				return fmt.Errorf("EventID is not a valid UUID: %s", err)
			} else if !uuid.Equal(id, eventID) {
				return fmt.Errorf("Unexpected EventID: got=%s", v)
			}

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
		WithBackOff(StopBackOff),
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

func TestBackOff(t *testing.T) {
	counter := 0

	srvr, err := testServer(&workflowServer{
		getJob: func(getJob workflow.Workflow_getJob) error {
			counter++
			return errors.New("ERROR")
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
		WithBackOff(func() backoff.BackOff {
			cb := &backoff.ZeroBackOff{}
			return backoff.WithMaxRetries(cb, 5)
		}),
	)
	if err != nil {
		t.Fatalf("Could not initialize new raven worker: %s", err.Error())
	}

	// we are expecting an error
	if _, err := w.Consume(); err == nil {
		t.Fatalf("Expected an error.")
	} else if err.Error() != "job.capnp:Workflow.getJob: rpc exception: ERROR" {
		t.Fatalf("Expected different error.")
	}

	if counter != 6 {
		t.Fatalf("Backoff failed %d", counter)
	}
}
