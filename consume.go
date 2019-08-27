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
// Messages can be acknowledged by using Ack(message)
//
//     message, err := w.Consume()
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
func (c *Worker) Consume() (Message, error) {
	var t *time.Timer

	cb := backoff.NewExponentialBackOff()

	for {
		message, err := c.consume()
		if err == nil {
			return message, nil
		}

		next := cb.NextBackOff()
		if next == backoff.Stop {
			c.l.Errorf("Could not consume message for: %d: %s", zap.Duration("backoff", cb.GetElapsedTime()), err)
			return EmptyMessage, err
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

// TODO: proper naming
type event struct {
	MetaData map[string]interface{} `json:"metadata"`

	Content string `json:"content"`
}

func (c *Worker) consume() (Message, error) {
	reference, err := c.waitForWork()
	if err != nil {
		return EmptyMessage, err
	}

	evt, err := c.getEvent(reference)
	if err != nil {
		return EmptyMessage, err
	}

	return Message{
		ref:      reference,
		content:  []byte(evt.Content),
		metaData: evt.MetaData,
	}, nil
}

func (c *Worker) getEvent(ref *Reference) (*event, error) {
	// create the request
	req, err := c.newRequest(http.MethodGet, path.Join("flow", c.FlowID, "events", ref.EventID), nil)
	if err != nil {
		return nil, err
	}

	// Do the request
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	// In the response is the reference to the message
	evt := event{}

	// Decode into Reference
	if err := json.NewDecoder(resp.Body).Decode(&evt); err != nil {
		return nil, err
	}

	return &evt, nil
}

// getWork will ask the server for work
func (c *Worker) getWork() (*Reference, error) {
	req, err := c.newRequest(http.MethodGet, path.Join("workers", c.WorkerID, "work"), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	// In the response is the reference to the message
	ref := Reference{}

	// Decode into Reference
	if err := json.NewDecoder(resp.Body).Decode(&ref); err != nil {
		return nil, err
	}

	return &ref, nil
}

// waitForWork will wait for work. If no work is available it will retry.
func (c *Worker) waitForWork() (*Reference, error) {
	var t *time.Timer

	cb := backoff.NewConstantBackOff(time.Millisecond * 200)

	for {
		reference, err := c.getWork()
		if err == nil {
			return reference, nil
		}

		// we will loop only if no messages available
		if err != ErrNotFound {
			return nil, err
		}

		if next := cb.NextBackOff(); next == backoff.Stop {
			return nil, err
		} else if t != nil {
			t.Reset(next)
		} else {
			t = time.NewTimer(next)
			defer t.Stop()
		}

		<-t.C
	}
}
