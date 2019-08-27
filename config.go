package ravenworker

import (
	"errors"
)

type Config struct {
	urls []string

	WorkerID string
	FlowID   string

	l Logger
}

func (c Config) validate() error {
	if len(c.urls) == 0 {
		return errors.New("env RAVEN_URL needs to be set")
	}

	if c.FlowID == "" {
		return errors.New("env FLOW_ID needs to be set")
	}

	if c.WorkerID == "" {
		return errors.New("env WORKER_ID needs to be set")
	}

	return nil
}
