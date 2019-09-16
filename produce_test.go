package ravenworker

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cenkalti/backoff"
	"github.com/dutchsec/raven-worker/workflow"
	"github.com/google/go-cmp/cmp"
	uuid "github.com/satori/go.uuid"
)

var (
	TestProduceMessage = Message{
		MetaData: []Metadata{
			Metadata{
				Key:   "test",
				Value: "test",
			},
		},
		Content: Content("content"),
	}
)

func StopBackOff() backoff.BackOff {
	return &backoff.StopBackOff{}
}

func TestProduce(t *testing.T) {
	srvr, err := testServer(&workflowServer{
		putEvent: func(putEvent workflow.Workflow_putEvent) error {
			if v, err := putEvent.Params.FlowID(); err != nil {
				return err
			} else if id, err := uuid.FromBytes(v); err != nil {
				return fmt.Errorf("FlowID is not a valid UUID: %s", err)
			} else if !uuid.Equal(id, flowID) {
				return fmt.Errorf("Unexpected FlowID: got=%s", v)
			}

			evt, err := putEvent.Params.Event()
			if err != nil {
				return err
			}

			meta, err := evt.Meta()
			if err != nil {
				return err
			}

			metadata := transformMeta(meta)

			if diff := cmp.Diff(metadata, TestProduceMessage.MetaData); diff != "" {
				return fmt.Errorf("putEvent() metadata mismatch (-want +got):\n%s", diff)
			}

			content, err := evt.Content()
			if err != nil {
				return err
			}

			if !bytes.Equal(content, TestProduceMessage.Content) {
				return fmt.Errorf("putEvent() content mismatch want:%s got:%s", string(content), string(TestProduceMessage.Content))
			}

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

	if err := w.Produce(TestProduceMessage); err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}
}
