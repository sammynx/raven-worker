package ravenworker

import (
	"net/http"
	"net/url"
	"sync"
)

// NewRoundRobinTransport will initialize a new http tranport
// that will cycle through multiple urls. Together with the
// backoff this will supply fault handling and load balancing.
func NewRoundRobinTransport(urls ...url.URL) http.RoundTripper {
	return &roundRobinTransport{
		urls: urls,

		RoundTripper: http.DefaultTransport,
	}
}

type roundRobinTransport struct {
	urls  []url.URL
	count int
	m     sync.Mutex

	http.RoundTripper
}

func (t *roundRobinTransport) Pick() url.URL {
	t.m.Lock()
	defer t.m.Unlock()

	t.count++
	return t.urls[t.count%len(t.urls)]
}

func (t *roundRobinTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := t.Pick()

	req.Host = host.Host
	req.URL.Scheme = host.Scheme
	req.URL.Host = host.Host

	return t.RoundTripper.RoundTrip(req)
}
