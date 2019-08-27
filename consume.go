package ravenworker

import (
	"encoding/json"
	"net/http"
	"path"
	"time"

	"github.com/cenkalti/backoff"
	"go.uber.org/zap"
)

var EmptyMessage = Message{}

// Consume retrieves a message from workflow. When there are
// no messages available, it will retry with a constant interval.
//
// Messages can be retrieved by using Get(message)

// Messages can be acknowledged by using Ack(ref)
//
//     ref, err := w.Consume()
//     if err !=nil {
//         panic(err)
//     }
//
//     message, err := w.Get(ref)
//     if err !=nil {
//         panic(err)
//     }
//
//     err := w.Ack(*message)
//     if err !=nil {
//         panic(err)
//     }
//
// Use this function for the 'transform' and 'load' worker types.
func (c *DefaultWorker) Consume() (Reference, error) {
	var t *time.Timer

	cb := backoff.NewExponentialBackOff()

	for {
		reference, err := c.waitForWork()
		if err == nil {
			return reference, nil
		}

		next := cb.NextBackOff()
		if next == backoff.Stop {
			c.l.Errorf("Could not consume message for: %d: %s", zap.Duration("backoff", cb.GetElapsedTime()), err)
			return Reference{}, err
		} else if t != nil {
			t.Reset(next)
		} else {
			t = time.NewTimer(next)
			defer t.Stop()
		}

		c.l.Debugf("Got error while consuming message for: %s. Will retry in %v.", err.Error(), next)

		<-t.C
	}
}

// getWork will ask the server for work
func (c *DefaultWorker) getWork() (Reference, error) {
	req, err := c.newRequest(http.MethodGet, path.Join("workers", c.WorkerID, "work"), nil)
	if err != nil {
		return Reference{}, err
	}

	resp, err := c.do(req)
	if err != nil {
		return Reference{}, err
	}

	defer resp.Body.Close()

	// In the response is the reference to the message
	ref := Reference{}

	// Decode into Reference
	if err := json.NewDecoder(resp.Body).Decode(&ref); err != nil {
		return Reference{}, err
	}

	return ref, nil
}

// waitForWork will wait for work. If no work is available it will retry.
func (c *DefaultWorker) waitForWork() (Reference, error) {
	var t *time.Timer

	cb := backoff.NewConstantBackOff(time.Millisecond * 200)

	for {
		reference, err := c.getWork()
		if err == nil {
			return reference, nil
		}

		// we will loop only if no messages available
		if err != ErrNotFound {
			return Reference{}, err
		}

		if next := cb.NextBackOff(); next == backoff.Stop {
			return Reference{}, err
		} else if t != nil {
			t.Reset(next)
		} else {
			t = time.NewTimer(next)
			defer t.Stop()
		}

		<-t.C
	}
}
