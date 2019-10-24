package ravenworker

import (
	"bytes"
	"errors"
	"net/http"

	"github.com/cenkalti/backoff"
)

type LogUploader struct {
	endpoint string
}

//Write create a POST request to the set endpoint with p as its body.
func (l *LogUploader) Write(p []byte) (n int, err error) {
	n = len(p)

	bpost := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)

	body := bytes.NewBuffer(p)

	err = backoff.Retry(func() error {
		resp, err := http.Post(l.endpoint, "application/json", body)
		if err == nil {
			if resp.StatusCode != 200 {
				n = 0
				err = errors.New(resp.Status)
			}
			defer resp.Body.Close()
		}
		return err
	}, bpost)

	return
}
