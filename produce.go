package ravenworker

import (
	"net/http"
	"path"
	"time"

	"io/ioutil"

	"github.com/cenkalti/backoff"
	"go.uber.org/zap"
)

type EventID string

func (e EventID) String() string {
	return string(e)
}

// Produce will store a new message in the queue. If an
// error occurs it will retry using exponential backoff
// strategy.
//
//     message := NewMessage()
//         .Content([]byte("test"))
//
//    err := w.Produce(message)
//    if err != nil {
//        panic (err)
//    }
func (c *Worker) Produce(message Message) error {
	var t *time.Timer

	cb := backoff.NewExponentialBackOff()

	for {
		err := c.produce(message)
		if err == nil {
			return nil
		}

		next := cb.NextBackOff()
		if next == backoff.Stop {
			c.l.Errorf("Could not produce message for: %d: %s", zap.Duration("backoff", cb.GetElapsedTime()), err)
			return err
		} else if t != nil {
			t.Reset(next)
		} else {
			t = time.NewTimer(next)
			defer t.Stop()
		}

		c.l.Debugf("Got error while producing message: %s. Will retry in %v.", err.Error(), next)

		<-t.C
	}
}

func (c *Worker) produce(message Message) error {
	eventID, err := c.getNewEventID()
	if err != nil {
		return err
	}

	if err := c.putEvent(eventID, message); err != nil {
		return err
	}

	return nil
}

// getEventID will retrieve a new event id from workflow
func (c *Worker) getNewEventID() (EventID, error) {
	// create the request
	req, err := c.newRequest(http.MethodPost, path.Join("flow", c.FlowID, "events"), nil)
	if err != nil {
		return "", err
	}

	// Do the request
	resp, err := c.do(req)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return EventID(data), nil
}

// putEvent will put the new message with event id to workflow
func (c *Worker) putEvent(eventID EventID, message Message) error {
	body := JsonReader(message)

	req, err := c.newRequest(http.MethodPut, path.Join("flow", c.FlowID, "events", eventID.String()), body)
	if err != nil {
		return err
	}

	resp, err := c.do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return nil
}
