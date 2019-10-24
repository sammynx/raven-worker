package ravenworker

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/dutchsec/raven-worker/workflow"
	"zombiezen.com/go/capnproto2/rpc"
)

type Worker interface {
	Consume(ctx context.Context) (Reference, error)
	Get(Reference) (Message, error)
	Ack(Reference, ...AckOptionFunc) error
	Produce(Message) error
}

type DefaultWorker struct {
	Config

	w *workflow.Workflow
	m sync.Mutex

	connectionCounter int
}

func (w *DefaultWorker) connect() error {
	w.m.Lock()
	defer w.m.Unlock()

	u := w.urls[w.connectionCounter%len(w.urls)]

	w.log.Infof("Connecting to rpc server: %s", u)

	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		return err
	}

	rpcconn := rpc.NewConn(rpc.StreamTransport(conn))

	client := rpcconn.Bootstrap(context.Background())

	w.w = &workflow.Workflow{Client: client}

	w.connectionCounter++
	return err
}

// New returns a new configured Raven Worker client
func New(opts ...OptionFunc) (Worker, error) {
	c := Config{
		newBackOff: func() backoff.BackOff {
			return backoff.NewExponentialBackOff()
		},
		consumeTimeout: 10 * time.Second,
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
