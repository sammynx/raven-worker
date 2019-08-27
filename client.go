package ravenworker

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const defaultHTTPTimeout = 5 * time.Second

var (
	ErrNotFound = errors.New("Not found")
)

// newRequest will return a http request for path relative to the base url
func (c *Worker) newRequest(method string, path string, body io.Reader) (*http.Request, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	return http.NewRequest(method, "/"+u.String(), body)
}

// do will do the actual request, using the http client
func (c *Worker) do(req *http.Request) (*http.Response, error) {
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "raven-worker/1.0")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		// ok
	} else if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	} else {
		return nil, fmt.Errorf("Bad status returned. req: %s, Status: %s", req.URL.String(), resp.Status)
	}

	return resp, nil
}
