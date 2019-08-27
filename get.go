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
		cr, err := c.get(ref)
		if err == nil {
			return Message{
				content:  []byte(cr.Content),
				metaData: cr.MetaData,
			}, nil
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

type getResponse struct {
	MetaData map[string]interface{} `json:"metadata"`

	Content string `json:"content"`
}

func (c *DefaultWorker) get(ref Reference) (*getResponse, error) {
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
	cr := getResponse{}

	// Decode into Reference
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return nil, err
	}

	return &cr, nil
}
