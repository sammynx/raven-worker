package ravenworker

import (
	"time"

	"github.com/cenkalti/backoff"
	"github.com/dutchsec/raven-worker/workflow"
	uuid "github.com/satori/go.uuid"
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

	cb := c.newBackOff()

	for {
		m, err := c.get(ref)
		if err == nil {
			return m, nil
		}

		next := cb.NextBackOff()
		if next == backoff.Stop {
			c.l.Errorf("Could not get message: %s", err)
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

func transformMeta(md workflow.Event_Metadata_List) []Metadata {
	metadata := make([]Metadata, md.Len())

	for i := range metadata {
		key, _ := md.At(i).Key()
		metadata[i].Key = key

		value, _ := md.At(i).Value()
		metadata[i].Value = value
	}

	return metadata
}

func (c *DefaultWorker) get(ref Reference) (Message, error) {
	eventID, _ := uuid.FromString(ref.EventID)

	res, err := c.w.GetEvent(context.Background(), func(params workflow.Connection_getEvent_Params) error {
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

	metadata := transformMeta(meta)

	return Message{
		Content:  content,
		MetaData: metadata,
	}, nil
}
