package ravenworker

import (
	"encoding/json"
	"io"
	"net"

	context "golang.org/x/net/context"
	capnp "zombiezen.com/go/capnproto2"
	"zombiezen.com/go/capnproto2/rpc"
)

type Worker interface {
	Consume() (Reference, error)
	Get(Reference) (Message, error)
	Ack(Reference, ...AckOptionFunc) error
	Produce(Message) error
}

type DefaultWorker struct {
	Config

	w *Workflow
}

/*
type ReconnectingWorkflow struct {
	w *Workflow
}

func (rw *ReconnectingWorkflow) PutEvent(ctx context.Context, fn func(params Workflow_putEvent_Params) error) {
	if err := rw.w.PutEvent(ctx, fn); err == io.EOF {
		// reconnect
		// rw.reconnect()
		// retry will be handled by backoff timer
	}
}

func (rw *ReconnectingWorkflow) connect() (net.Conn, error) {
	// reconnect
	// use next host
	// capnproto://127.0.0.1:8023

	conn, _ := net.Dial("tcp", "127.0.0.1:8023")

	rpcconn := rpc.NewConn(rpc.StreamTransport(conn))
	rw.w = Workflow{Client: rpcconn.Bootstrap(context.Background())}
}
*/

// TODO: connection error handling
type ReconnectingClient struct {
	capnp.Client
}

func (rc ReconnectingClient) Call(call *capnp.Call) capnp.Answer {
	answer := rc.Client.Call(call)
	return answer
}

// New returns a new configured Raven Worker client
func New(opts ...OptionFunc) (Worker, error) {
	c := Config{
		l: DefaultLogger,
	}

	for _, optFn := range opts {
		if err := optFn(&c); err != nil {
			return nil, err
		}
	}

	if err := c.validate(); err != nil {
		return nil, err
	}

	// TODO: how and when to reconnect?
	conn, _ := net.Dial("tcp", "127.0.0.1:8023")

	rpcconn := rpc.NewConn(rpc.StreamTransport(conn))

	client := ReconnectingClient{rpcconn.Bootstrap(context.Background())}

	w := Workflow{Client: client}

	return &DefaultWorker{
		Config: c,

		w: &w,
	}, nil
}

// JsonReader will return a json encoder of msg
func JsonReader(v interface{}) io.ReadCloser {
	pr, pw := io.Pipe()

	go func() {
		err := json.NewEncoder(pw).Encode(v)
		pw.CloseWithError(err)
	}()

	return pr
}
