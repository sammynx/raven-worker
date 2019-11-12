package ravenworker

import (
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/dutchsec/raven-worker/workflow"
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

	cb := c.newBackOff()

	for {
		err := c.produce(message)
		if err == nil {
			return nil
		}

		next := cb.NextBackOff()
		if next == backoff.Stop {
			c.log.Errorf("Could not produce message: %s", err)
			return err
		} else if t != nil {
			t.Reset(next)
		} else {
			t = time.NewTimer(next)
			defer t.Stop()
		}

		c.log.Debugf("Got error while producing message: %s. Will retry in %v.", err.Error(), next)

		<-t.C
	}
}

func (c *DefaultWorker) produce(message Message) error {
	_, err := c.w.PutNewEvent(context.Background(), func(params workflow.Connection_putNewEvent_Params) error {
		_, seg, _ := capnp.NewMessage(capnp.SingleSegment(nil))

		capnpEvent, _ := workflow.NewRootEvent(seg)

		capnpEvent.SetFilter(false)

		if err := capnpEvent.SetContent(message.Content); err != nil {
			return err
		}

		eventMetadataList, _ := workflow.NewEvent_Metadata_List(seg, int32(len(message.MetaData)))

		for i := range message.MetaData {
			meta, _ := workflow.NewEvent_Metadata(seg)
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
