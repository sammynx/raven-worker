package ravenworker

import (
	"time"

	"github.com/cenkalti/backoff"
	"go.uber.org/zap"
	context "golang.org/x/net/context"
	capnp "zombiezen.com/go/capnproto2"
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
func (c *DefaultWorker) Produce(message Message) error {
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

func (c *DefaultWorker) produce(message Message) error {
	_, err := c.w.PutEvent(context.Background(), func(params Workflow_putEvent_Params) error {
		if err := params.SetFlowID(c.FlowID.Bytes()); err != nil {
			return err
		}

		_, seg, _ := capnp.NewMessage(capnp.SingleSegment(nil))

		capnpEvent, _ := NewRootEvent(seg)

		capnpEvent.SetFilter(false)

		if err := capnpEvent.SetContent(message.Content); err != nil {
			return err
		}

		eventMetadataList, _ := NewEvent_Metadata_List(seg, int32(len(message.MetaData)))

		for i := range message.MetaData {
			meta, _ := NewEvent_Metadata(seg)
			_ = meta.SetKey(message.MetaData[i].Key)
			_ = meta.SetValue(message.MetaData[i].Value)
			_ = eventMetadataList.Set(i, meta)
		}

		if err := capnpEvent.SetMeta(eventMetadataList); err != nil {
			return err
		}

		return params.SetEvent(capnpEvent)
	}).Struct()

	return err
}
