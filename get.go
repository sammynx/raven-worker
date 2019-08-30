package ravenworker

import (
	"time"

	"github.com/cenkalti/backoff"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
	context "golang.org/x/net/context"
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
	eventID, _ := uuid.FromString(ref.EventID)

	res, err := c.w.GetEvent(context.Background(), func(params Workflow_getEvent_Params) error {
		return params.SetEventID(eventID.Bytes())
	}).Struct()

	if err != nil {
		return Message{}, err
	}

	event, err := res.Event()
	if err != nil {
		return Message{}, err
	}

	content, err := event.Content()
	if err != nil {
		return Message{}, err
	}

	meta, err := event.Meta()
	if err != nil {
		return Message{}, err
	}

	metadata := make([]Metadata, meta.Len())

	for i := range metadata {
		key, _ := meta.At(i).Key()
		metadata[i].Key = key

		value, _ := meta.At(i).Value()
		metadata[i].Value = value
	}

	return Message{
		Content:  content,
		MetaData: metadata,
	}, nil
}
