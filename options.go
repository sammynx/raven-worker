package ravenworker

import (
	"net/url"
	"os"
	"strings"
)

type OptionFunc func(*Config) error

func WithRavenURL(urlStr string) (OptionFunc, error) {
	parts := strings.Split(urlStr, ",")

	urls := make([]string, len(parts))
	for i, part := range parts {
		u, err := url.Parse(part)
		if err != nil {
			return nil, err
		}

		urls[i] = u.String()
	}

	return func(c *Config) error {
		c.urls = urls
		return nil
	}, nil
}

func WithFlowID(flowID string) (OptionFunc, error) {
	return func(c *Config) error {
		c.FlowID = flowID
		return nil
	}, nil
}

func WithWorkerID(workerID string) (OptionFunc, error) {
	return func(c *Config) error {
		c.WorkerID = workerID
		return nil
	}, nil
}

func WithLogger(l Logger) (OptionFunc, error) {
	return func(c *Config) error {
		c.l = l
		return nil
	}, nil
}

// errorFunc will pass the initialization error through
func errorFunc(err error) OptionFunc {
	return func(c *Config) error {
		return err
	}
}

func DefaultEnvironment() OptionFunc {
	opts := []OptionFunc{}

	if optionFn, err := WithRavenURL(os.Getenv("RAVEN_URL")); err != nil {
		return errorFunc(err)
	} else {
		opts = append(opts, optionFn)
	}

	if optionFn, err := WithFlowID(os.Getenv("FLOW_ID")); err != nil {
		return errorFunc(err)
	} else {
		opts = append(opts, optionFn)
	}

	if optionFn, err := WithWorkerID(os.Getenv("WORKER_ID")); err != nil {
		return errorFunc(err)
	} else {
		opts = append(opts, optionFn)
	}

	return func(c *Config) error {
		for _, optionFn := range opts {
			if err := optionFn(c); err != nil {
				return err
			}
		}

		return nil
	}
}
