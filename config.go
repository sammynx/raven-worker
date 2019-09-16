package ravenworker

import (
	"errors"
	"net/url"

	uuid "github.com/satori/go.uuid"
)

type Config struct {
	urls []url.URL

	WorkerID uuid.UUID

	FlowID uuid.UUID

	l Logger

	newBackOff BackOffFunc
}

func (c Config) validate() error {
	if len(c.urls) == 0 {
		return errors.New("env RAVEN_URL needs to be set")
	}

	if uuid.Equal(c.FlowID, uuid.Nil) {
		return errors.New("env FLOW_ID needs to be set")
	}

	if uuid.Equal(c.WorkerID, uuid.Nil) {
		return errors.New("env WORKER_ID needs to be set")
	}

	return nil
}
