package ravenworker

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"testing"

	uuid "github.com/satori/go.uuid"
	"zombiezen.com/go/capnproto2/rpc"
)

var (
	flowID   = uuid.NewV4()
	workerID = uuid.NewV4()
)

// important to know is that you
// cannot return t.Fatalf / t.FailNow()
// from within go routines
func TestMain(m *testing.M) {

	// call flag.Parse() here if TestMain uses flags
	os.Exit(m.Run())
}

type workflowServer struct {
	getEvent       func(getEvent Workflow_getEvent) error
	getJob         func(getJob Workflow_getJob) error
	ackJob         func(ackJob Workflow_ackJob) error
	putEvent       func(putEvent Workflow_putEvent) error
	getLatestEvent func(getLatestEvent Workflow_getLatestEvent) error
}

func (w *workflowServer) PutEvent(putEvent Workflow_putEvent) error {
	if w.putEvent != nil {
		return w.putEvent(putEvent)
	}

	return fmt.Errorf("putEvent not configured")
}

func (w *workflowServer) AckJob(ackJob Workflow_ackJob) error {
	if w.ackJob != nil {
		return w.ackJob(ackJob)
	}

	return fmt.Errorf("ackJob not configured")
}

func (w *workflowServer) GetJob(getJob Workflow_getJob) error {
	if w.getJob != nil {
		return w.getJob(getJob)
	}

	return fmt.Errorf("getJob not configured")
}

func (w *workflowServer) GetLatestEvent(getLatestEvent Workflow_getLatestEvent) error {
	if w.getLatestEvent != nil {
		return w.getLatestEvent(getLatestEvent)
	}

	return fmt.Errorf("getLatestEvent not configured")
}

func (w *workflowServer) GetEvent(getEvent Workflow_getEvent) error {
	if w.getEvent != nil {
		return w.getEvent(getEvent)
	}

	return fmt.Errorf("getEvent not configured")
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
				wfsc := Workflow_ServerToClient(ws)

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

	if ref.AckID != ackID.String() {
		t.Errorf("AckID isn't right. Got %s, want %s", ref.AckID, ackID.String())
	}

	if ref.EventID != eventID.String() {
		t.Errorf("EventID isn't right. Got %s, want %s", ref.EventID, eventID.String())
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

	if ref.AckID != ackID.String() {
		t.Errorf("AckID isn't right. Got %s, want %s", ref.AckID, ackID.String())
	}

	if ref.EventID != eventID.String() {
		t.Errorf("EventID isn't right. Got %s, want %s", ref.EventID, eventID.String())
	}

	if err := w.Ack(ref, WithMessage(msg)); err != nil {
		t.Errorf("Ack failed: %s", err.Error())
	}
}

// TODO: update tests to use capnproto
//

/*
// TODO: change this to use mux
// and have mux.Handle() for paths
func DefaultConsumeHandler(t *testing.T, w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/workers/workerid/work" {
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

	w, err := New(
		MustWithRavenURL(ts.URL),
		MustWithFlowID("flowid"),
		MustWithWorkerID("workerid"),
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
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	want := TestConsumeMessage
	got := map[string]interface{}{
		"metadata": message.MetaData,
		"content":  string(message.Content),
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Consume() mismatch (-want +got):\n%s", diff)
	}

	if err := w.Ack(ref, WithMessage(message)); err != nil {
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

	ref, err := w.Consume()
	if err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	message, err := w.Get(ref)
	if err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	want := TestConsumeMessage
	got := map[string]interface{}{
		"metadata": message.MetaData,
		"content":  string(message.Content),
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Consume() mismatch (-want +got):\n%s", diff)
	}

	message.Content = Content([]byte("updated content"))

	if err := w.Ack(ref, WithMessage(message)); err != nil {
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

	ref, err := w.Consume()
	if err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	message, err := w.Get(ref)
	if err != nil {
		t.Fatalf("Could not consume message: %s", err.Error())
	}

	want := TestConsumeMessage
	got := map[string]interface{}{
		"metadata": message.MetaData,
		"content":  string(message.Content),
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Consume() mismatch (-want +got):\n%s", diff)
	}

	if err := w.Ack(ref, WithMessage(message), WithFilter()); err != nil {
		t.Fatalf("Could not ack message: %s", err.Error())
	}
}
*/
