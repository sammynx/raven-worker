package ravenworker

import (
	"context"

	"github.com/cenkalti/backoff"
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

	ctxTimeout, cancel := context.WithTimeout(ctx, c.consumeTimeout)
	defer cancel()

	cb := backoff.WithContext(c.newBackOff(), ctxTimeout)
	var ref Reference

	operation := func() error {
		var err error
		ref, err = c.getWork()
		return err
	}

	// retry until no error is returned and timeout is not expired.
	err := backoff.Retry(operation, cb)

	return ref, err
}

func (c *DefaultWorker) getWork() (Reference, error) {
	res, err := c.w.GetJob(context.Background(), func(params workflow.Workflow_getJob_Params) error {
		return params.SetWorkerID(c.WorkerID.Bytes())
	}).Struct()

	if err != nil {
		return Reference{}, err
	}

	ackID, _ := res.AckID()
	ackUUID, _ := uuid.FromBytes(ackID)

	eventID, _ := res.EventID()
	eventUUID, _ := uuid.FromBytes(eventID)

	return Reference{
		AckID:   ackUUID.String(),
		EventID: eventUUID.String(),
	}, nil
}

/*

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
*/
