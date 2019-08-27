package ravenworker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/cenkalti/backoff"
	"go.uber.org/zap"
)

type ackOptionFunc func(r *ackRequest) error

// WithFilter will keep the flow from processing further.
func WithFilter() ackOptionFunc {
	return func(r *ackRequest) error {
		r.Filter = true
		return nil
	}
}

type ackRequest struct {
	Content  string
	Metadata map[string]interface{}
	Filter   bool
}

func (r *ackRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Content  string                 `json:"content,omitempty"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
		Filter   bool                   `json:"filter"`
	}{
		Content:  string(r.Content),
		Metadata: r.Metadata,
		Filter:   r.Filter,
	})
}

// Ack will acknowledge the message, only consumed messages are
// allowed.
//
// WithFilter() will filter further processing
func (c *Worker) Ack(message Message, options ...ackOptionFunc) error {
	// default ackRequest
	ar := ackRequest{
		Content:  string(message.content),
		Metadata: message.metaData,
		Filter:   false,
	}

	// fill with options
	for _, optionFn := range options {
		if err := optionFn(&ar); err != nil {
			return err
		}
	}

	if message.ref == nil {
		return fmt.Errorf("Cannot ack a message without reference, shouldn't you use produce instead?")
	}

	var t *time.Timer

	cb := backoff.NewExponentialBackOff()

	for {
		err := c.ack(*message.ref, ar)
		if err == nil {
			return nil
		}

		next := cb.NextBackOff()
		if next == backoff.Stop {
			c.l.Errorf("Could not ack message for: %d: %s", zap.Duration("backoff", cb.GetElapsedTime()), err)
			return err
		} else if t != nil {
			t.Reset(next)
		} else {
			t = time.NewTimer(next)
			defer t.Stop()
		}

		c.l.Debugf("Got error while ack message: %s. Will retry in %v.", err.Error(), next)

		<-t.C
	}

	return nil
}

func (c *Worker) ack(ref Reference, ar ackRequest) error {
	body := JsonReader(ar)

	// create the request
	req, err := c.newRequest(http.MethodPut, path.Join("workers", c.WorkerID, "ack", ref.AckID), body)
	if err != nil {
		return err
	}

	// Do the request
	resp, err := c.do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return nil
}
