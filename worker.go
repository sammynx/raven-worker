package ravenworker

import (
	"net"
	"sync"

	"github.com/cenkalti/backoff"
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

	w *Workflow
	m sync.Mutex

	connectionCounter int
}

func (w *DefaultWorker) isConnected() {

}

func (w *DefaultWorker) connect() error {
	w.m.Lock()
	defer w.m.Unlock()

	u := w.urls[w.connectionCounter%len(w.urls)]

	log.Infof("Connecting to rpc server: %s\n", u)

	// TODO: how and when to reconnect?
	// capnp.IsErrorClient(c capnp.Client)
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		return err
	}

	rpcconn := rpc.NewConn(rpc.StreamTransport(conn))

	client := rpcconn.Bootstrap(context.Background())

	w.w = &Workflow{Client: client}

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
