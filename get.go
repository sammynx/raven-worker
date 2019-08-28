package ravenworker

import (
	"encoding/json"
	"net/http"
	"path"
	"time"

	"github.com/cenkalti/backoff"
	"go.uber.org/zap"
)

// Get will retrieve the event for reference.
//
//     msg,  err := Get(ref)
//     if err != nil {
//         // handle error
//     }
func (c *DefaultWorker) Get(ref Reference) (Message, error) {
	var t *time.Timer

	cb := backoff.NewExponentialBackOff()

	for {
		m, err := c.get(ref)
		if err == nil {
			return m, nil
		}

		next := cb.NextBackOff()
		if next == backoff.Stop {
			c.l.Errorf("Could not get message for: %d: %s", zap.Duration("backoff", cb.GetElapsedTime()), err)
			return Message{}, err
		} else if t != nil {
			t.Reset(next)
		} else {
			t = time.NewTimer(next)
			defer t.Stop()
		}

		c.l.Debugf("Got error while get message for: %s. Will retry in %v.", err.Error(), next)

		<-t.C
	}
}

func (c *DefaultWorker) get(ref Reference) (Message, error) {
	// create the request
	req, err := c.newRequest(http.MethodGet, path.Join("flow", c.FlowID, "events", ref.EventID), nil)
	if err != nil {
		return Message{}, err
	}

	// Do the request
	resp, err := c.do(req)
	if err != nil {
		return Message{}, err
	}

	defer resp.Body.Close()

	m := Message{}

	// Decode into Reference
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return Message{}, err
	}

	return m, nil
}
