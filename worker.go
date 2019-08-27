package ravenworker

import (
	"encoding/json"
	"io"
	"net/http"
)

type Worker struct {
	Config

	client *http.Client
}

// New returns a new configured Raven Worker client
func New(opts ...OptionFunc) (*Worker, error) {
	c := Config{
		l: DefaultLogger,
	}

	for _, optFn := range opts {
		if err := optFn(&c); err != nil {
			return nil, err
		}
	}

	if err := c.validate(); err != nil {
		return nil, err
	}

	/*
		// Get a balancer that performs round-robin scheduling between two servers.
		balancer, err := roundrobin.NewBalancerFromURL(c.urls...)
		if err != nil {
			return nil, err
		}

		c2, err := balancer.Get()
		if err != nil {
			return nil, err
		}
		fmt.Println(c2.URL())
		// Get a HTTP client based on that balancer.
		client := balancers.NewClient(balancer)
	*/
	client := &http.Client{
		Timeout: defaultHTTPTimeout,
	}

	return &Worker{
		Config: c,

		client: client,
	}, nil
}

// JsonReader will return a json encoder of msg
func JsonReader(v interface{}) io.ReadCloser {
	pr, pw := io.Pipe()

	go func() {
		err := json.NewEncoder(pw).Encode(v)
		pw.CloseWithError(err)
	}()

	return pr
}
