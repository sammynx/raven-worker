package ravenworker

import (
	"time"

	"github.com/cenkalti/backoff"
	"github.com/dutchsec/raven-worker/workflow"
	uuid "github.com/satori/go.uuid"
	context "golang.org/x/net/context"
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
func (c *DefaultWorker) Consume() (Reference, error) {
	var t *time.Timer

	cb := c.newBackOff()

	for {
		reference, err := c.waitForWork()
		if err == nil {
			return reference, nil
		}

		next := cb.NextBackOff()
		if next == backoff.Stop {
			c.l.Errorf("Could not consume message: %s", err)
			return Reference{}, err
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

func IsNotFoundErr(err error) bool {
	return err.Error() == "job.capnp:Workflow.getJob: rpc exception: item not found"
}

// waitForWork will wait for work. If no work is available it will retry.
func (c *DefaultWorker) waitForWork() (Reference, error) {
	var t *time.Timer

	// new constant backoff with infinite retries, this could
	// be changed in the future to automatically stop workers
	// if no messages have been seen for a while. Formation
	// should restart the worker if backlog is growing.
	var cb backoff.BackOff
	cb = backoff.NewConstantBackOff(time.Millisecond * 200)

	for {
		res, err := c.w.GetJob(context.Background(), func(params workflow.Workflow_getJob_Params) error {
			return params.SetWorkerID(c.WorkerID.Bytes())
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

		<-t.C
	}
}
