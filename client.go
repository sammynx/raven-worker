package ravenworker

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

const defaultHTTPTimeout = 5 * time.Second

var (
	ErrNotFound            = errors.New("Not Found")
	ErrNoContent           = errors.New("No Content")
	ErrInternalServerError = errors.New("Internal Server Error")
)

// newRequest will return a http request for path relative to the base url
func (c *DefaultWorker) newRequest(method string, path string, body io.Reader) (*http.Request, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	return http.NewRequest(method, "/"+u.String(), body)
}

// do will do the actual request, using the http client
func (c *DefaultWorker) do(req *http.Request) (*http.Response, error) {
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "raven-worker/1.0")

	if true {
	} else if data, err := httputil.DumpRequest(req, true); err == nil {
		fmt.Printf("Request dump: %s", string(data))
	} else {
		fmt.Println(err.Error())
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if true {
	} else if data, err := httputil.DumpResponse(resp, true); err == nil {
		fmt.Printf("Response dump: %s", string(data))
	} else {
		fmt.Println(err.Error())
	}

	if resp.StatusCode == http.StatusNoContent {
		return nil, ErrNoContent
	} else if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	} else if resp.StatusCode == http.StatusInternalServerError {
		return nil, ErrInternalServerError
	} else if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		// ok
	} else {
		return nil, fmt.Errorf("Bad status returned. req: %s, Status: %s", req.URL.String(), resp.Status)
	}

	return resp, nil
}
