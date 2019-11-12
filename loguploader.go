package ravenworker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"net/url"

	"github.com/cenkalti/backoff/v3"
)

// upload log messages to an http endpoint, implements 'io.Writer' interface.
type logUploader struct {
	sync.Mutex

	endpoint string
	buf      bytes.Buffer

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewLogUploader(ctx context.Context, endpoint string) (*logUploader, error) {
	if _, err := url.Parse(endpoint); err != nil {
		return nil, err
	}

	uctx, cancel := context.WithCancel(ctx)

	l := &logUploader{
		endpoint: endpoint,
		cancel:   cancel,
	}

	// start periodic uploading of log messages.
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		tick := time.Tick(1 * time.Second)

		for {
			select {
			case <-tick:
				l.send()
			case <-uctx.Done():
				// upload remaining logs in buffer.
				l.send()
				return
			}
		}
	}()

	return l, nil
}

//Write adds p to the upload buffer and is threadsafe.
func (l *logUploader) Write(p []byte) (n int, err error) {
	l.Lock()
	n, err = l.buf.Write(p)
	l.Unlock()
	return
}

//Close sends the last log messages and wait for it to finish.
func (l *logUploader) Close() error {
	l.cancel()
	l.wg.Wait()

	return nil
}

// send a batch of logs to the endpoint.
// if unsuccessfull it dumps them to stdout.
func (l *logUploader) send() {

	if l.buf.Len() == 0 {
		// nothing to do.
		return
	}

	// copy the buffer for quick release of the lock.
	l.Lock()

	body := make([]byte, l.buf.Len())
	copy(body, l.buf.Bytes())
	l.buf.Reset()

	l.Unlock()

	bpost := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)

	// post the collected logs.

	err := backoff.Retry(func() error {
		resp, err := http.Post(l.endpoint, "application/json", bytes.NewBuffer(body))
		if err == nil {
			if resp.StatusCode != 200 {
				return errors.New(resp.Status)
			}
			defer resp.Body.Close()
		}
		return err
	}, bpost)
	if err != nil {
		// dump logs to stdout.
		fmt.Printf("Upload Error: %v\n%s\n", err, string(body))
	}
}
