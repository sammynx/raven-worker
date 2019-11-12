package ravenworker

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/dutchsec/raven-worker/workflow"
	"github.com/gofrs/uuid"
)

var EmptyMessage = Message{}

// Consume retrieves a message from workflow. When there are
// no messages available, it will retry with a constant interval.
//
// Messages can be retrieved by using Get(message)
//
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
// blocks until it receives a message or 'consumeTimeout' expires.
func (c *DefaultWorker) Consume(ctx context.Context) (Reference, error) {

	// there is no timeout, we wait forever to get a reference.
	if c.consumeTimeout == 0 {
		return c.waitForWork(ctx)
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, c.consumeTimeout)
	defer cancel()

	return c.waitForWork(ctxTimeout)
}

func IsNotFoundErr(err error) bool {
	return err.Error() == "job.capnp:Workflow.getJob: rpc exception: item not found"
}

// waitForWork will wait for work. If no work is available it will retry
// until context is canceled.
func (c *DefaultWorker) waitForWork(ctx context.Context) (Reference, error) {
	var t *time.Timer

	// new constant backoff with infinite retries, this could
	// be changed in the future to automatically stop workers
	// if no messages have been seen for a while. Formation
	// should restart the worker if backlog is growing.
	//var cb backoff.BackOff
	//cb = backoff.NewConstantBackOff(time.Millisecond * 200)

	cb := c.newBackOff()

	for {
		res, err := c.w.GetJob(context.Background(), func(params workflow.Connection_getJob_Params) error {
			return nil
		}).Struct()

		if err == nil {
			ackID, _ := res.AckID()
			ackUUID, _ := uuid.FromBytes(ackID)

			eventID, _ := res.EventID()
			eventUUID, _ := uuid.FromBytes(eventID)

			return Reference{
				AckID:   ackUUID.String(),
				EventID: eventUUID.String(),
			}, nil
		}

		// we will loop only if no messages available
		if !IsNotFoundErr(err) {
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

		select {
		case <-ctx.Done():
			return Reference{}, ctx.Err()
		case <-t.C:
		}
	}
}
