package ravenworker

import (
	"net"
	"sync"

	"github.com/cenkalti/backoff"
	"github.com/dutchsec/raven-worker/workflow"
	"github.com/labstack/gommon/log"
	context "golang.org/x/net/context"
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

	w workflow.Connection

	m sync.Mutex

	connectionCounter int
}

func (w *DefaultWorker) connect() error {
	w.m.Lock()
	defer w.m.Unlock()

	u := w.urls[w.connectionCounter%len(w.urls)]

	log.Infof("Connecting to rpc server: %s", u)

	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		return err
	}

	rpcconn := rpc.NewConn(rpc.StreamTransport(conn))

	client := rpcconn.Bootstrap(context.Background())

	//TODO: workflowToServe?
	wf := &workflow.Workflow{Client: client}
	promise := wf.Connect(context.Background(), func(params workflow.Workflow_connect_Params) error {
		if err := params.SetFlowID(w.FlowID.Bytes()); err != nil {
			return err
		}

		if err := params.SetWorkerID(w.WorkerID.Bytes()); err != nil {
			return err
		}

		return nil
	})

	w.w = promise.Connection()

	w.connectionCounter++
	return err
}

// New returns a new configured Raven Worker client
func New(opts ...OptionFunc) (Worker, error) {
	c := Config{
		l: DefaultLogger,

		newBackOff: func() backoff.BackOff {
			return backoff.NewExponentialBackOff()
		},
	}

	for _, optFn := range opts {
		if err := optFn(&c); err != nil {
			return nil, err
		}
	}

	if err := c.validate(); err != nil {
		return nil, err
	}

	w := &DefaultWorker{
		Config: c,
	}

	// TODO: just start and have backoff handle
	if err := w.connect(); err != nil {
		return nil, err
	}

	return w, nil

}
