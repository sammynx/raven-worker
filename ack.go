package ravenworker

import (
	"time"

	"github.com/cenkalti/backoff"
	"github.com/dutchsec/raven-worker/workflow"
	"github.com/gofrs/uuid"
	context "golang.org/x/net/context"
)

type AckOptionFunc func(r *ackRequest) error

// WithFilter will keep the flow from processing further.
func WithFilter() AckOptionFunc {
	return func(r *ackRequest) error {
		r.Filter = true
		return nil
	}
}

// WithFilter will keep the flow from processing further.
func WithMessage(message Message) AckOptionFunc {
	return func(r *ackRequest) error {
		r.Content = message.Content
		r.Metadata = message.MetaData
		return nil
	}
}

type ackRequest struct {
	Content  []byte
	Metadata []Metadata
	Filter   bool
}

type Metadata struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Ack will acknowledge the message, only consumed messages are
// allowed.
//
// WithFilter() will filter further processing
func (c *DefaultWorker) Ack(ref Reference, options ...AckOptionFunc) error {
	// default ackRequest
	ar := ackRequest{
		Content:  nil,
		Metadata: nil,
		Filter:   false,
	}

	// fill with options
	for _, optionFn := range options {
		if err := optionFn(&ar); err != nil {
			return err
		}
	}

	var t *time.Timer

	cb := c.newBackOff()

	for {
		err := c.ack(ref, ar)
		if err == nil {
			return nil
		}

		next := cb.NextBackOff()
		if next == backoff.Stop {
			c.log.Errorf("Could not ack message: %s", err)
			return err
		} else if t != nil {
			t.Reset(next)
		} else {
			t = time.NewTimer(next)
			defer t.Stop()
		}

		c.log.Debugf("Got error while ack message: %s. Will retry in %v.", err.Error(), next)

		<-t.C
	}
}

func (c *DefaultWorker) ack(ref Reference, ar ackRequest) error {
	ackID, _ := uuid.FromString(ref.AckID)

	_, err := c.w.AckJob(context.Background(), func(params workflow.Workflow_ackJob_Params) error {
		e, err := params.NewEvent()
		if err != nil {
			return err
		}

		if err := e.SetContent([]byte(ar.Content)); err != nil {
			return err
		}

		eventMetadataList, _ := e.NewMeta(int32(len(ar.Metadata)))

		for i := range ar.Metadata {
			meta, _ := workflow.NewEvent_Metadata(e.Struct.Segment())
			_ = meta.SetKey(ar.Metadata[i].Key)
			_ = meta.SetValue(ar.Metadata[i].Value)
			_ = eventMetadataList.Set(i, meta)
		}

		if err := e.SetMeta(eventMetadataList); err != nil {
			return err
		}

		if err := params.SetEvent(e); err != nil {
			return err
		}

		if err := params.SetAckID(ackID.Bytes()); err != nil {
			return err
		}

		return nil
	}).Struct()

	return err
}
