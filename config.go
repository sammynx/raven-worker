package ravenworker

import (
	"errors"
	"io"
	"net/url"
	"time"

	"github.com/gofrs/uuid"
)

type Config struct {
	urls []url.URL

	WorkerID uuid.UUID

	FlowID uuid.UUID

	log Logger

	newBackOff BackOffFunc

	consumeTimeout time.Duration // time frame to wait for a new message. Zero is no timeout.

	maxIntake int // do not ingest more messages than this treshold.

	closers []io.Closer
}

func (c Config) validate() error {
	if len(c.urls) == 0 {
		return errors.New("env RAVEN_URL needs to be set")
	}

	if c.FlowID == uuid.Nil {
		return errors.New("env FLOW_ID needs to be set")
	}

	if c.WorkerID == uuid.Nil {
		return errors.New("env WORKER_ID needs to be set")
	}

	return nil
}
