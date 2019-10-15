package ravenworker

import (
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/dutchsec/raven-worker/workflow"
	"github.com/gofrs/uuid"
	"zombiezen.com/go/capnproto2/rpc"
)

func MustWithRavenURL(s string) OptionFunc {
	fn, err := WithRavenURL(s)
	if err != nil {
		panic(err)
	}
	return fn
}

func MustWithFlowID(flowID string) OptionFunc {
	fn, err := WithFlowID(flowID)
	if err != nil {
		panic(err)
	}
	return fn
}

func MustWithWorkerID(workerID string) OptionFunc {
	fn, err := WithWorkerID(workerID)
	if err != nil {
		panic(err)
	}
	return fn
}

var (
	flowID, _   = uuid.NewV4()
	workerID, _ = uuid.NewV4()
)

// important to know is that you
// cannot return t.Fatalf / t.FailNow()
// from within go routines
func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

type workflowServer struct {
	getEvent       func(getEvent workflow.Workflow_getEvent) error
	getJob         func(getJob workflow.Workflow_getJob) error
	ackJob         func(ackJob workflow.Workflow_ackJob) error
	putEvent       func(putEvent workflow.Workflow_putEvent) error
	getLatestEvent func(getLatestEvent workflow.Workflow_getLatestEventID) error
}

func (w *workflowServer) PutEvent(putEvent workflow.Workflow_putEvent) error {
	if w.putEvent != nil {
		return w.putEvent(putEvent)
	}

	return fmt.Errorf("putEvent not configured")
}

func (w *workflowServer) PutNewEvent(e workflow.Workflow_putNewEvent) error {
	return nil
}

func (w *workflowServer) AckJob(ackJob workflow.Workflow_ackJob) error {
	if w.ackJob != nil {
		return w.ackJob(ackJob)
	}

	return fmt.Errorf("ackJob not configured")
}

func (w *workflowServer) GetJob(getJob workflow.Workflow_getJob) error {
	if w.getJob != nil {
		return w.getJob(getJob)
	}

	return fmt.Errorf("getJob not configured")
}

func (w *workflowServer) GetLatestEventID(getLatestEvent workflow.Workflow_getLatestEventID) error {
	if w.getLatestEvent != nil {
		return w.getLatestEvent(getLatestEvent)
	}

	return fmt.Errorf("getLatestEvent not configured")
}

func (w *workflowServer) GetEvent(getEvent workflow.Workflow_getEvent) error {
	if w.getEvent != nil {
		return w.getEvent(getEvent)
	}

	return fmt.Errorf("getEvent not configured")
}

func (w *workflowServer) GetEventAllVersions(e workflow.Workflow_getEventAllVersions) error {
	return nil
}

func testServer(ws *workflowServer) (net.Listener, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("error starting listener: %s", err.Error())
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			go func() {
				wfsc := workflow.Workflow_ServerToClient(ws)

				connection := rpc.NewConn(
					rpc.StreamTransport(conn),
					rpc.MainInterface(wfsc.Client),
				)

				// Wait for connection to abort.
				<-connection.Done()
			}()
		}
	}()

	return l, nil
}
