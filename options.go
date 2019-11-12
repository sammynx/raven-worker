package ravenworker

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v3"

	"github.com/gofrs/uuid"
)

type OptionFunc func(*Config) error

func WithRavenURL(urlStr string) (OptionFunc, error) {
	parts := strings.Split(urlStr, ",")

	urls := make([]url.URL, len(parts))
	for i, part := range parts {
		u, err := url.Parse(part)
		if err != nil {
			return nil, err
		}

		urls[i] = *u
	}

	return func(c *Config) error {
		c.urls = urls
		return nil
	}, nil
}

func WithFlowID(s string) (OptionFunc, error) {
	flowID, err := uuid.FromString(s)
	if err != nil {
		return nil, err
	}

	return func(c *Config) error {
		c.FlowID = flowID
		return nil
	}, nil
}

func WithWorkerID(s string) (OptionFunc, error) {
	workerID, err := uuid.FromString(s)
	if err != nil {
		return nil, err
	}
	return func(c *Config) error {
		c.WorkerID = workerID
		return nil
	}, nil
}

func WithLogger(l Logger) (OptionFunc, error) {
	if l == nil {
		return nil, errors.New("WithLogger called with <nil> logger")
	}

	return func(c *Config) error {
		c.log = l

		// use Close if available.
		if deflog, ok := l.(io.Closer); ok {
			c.closers = append(c.closers, deflog)
		}
		return nil
	}, nil
}

type BackOffFunc func() backoff.BackOff

func WithBackOff(fn BackOffFunc) OptionFunc {
	return func(c *Config) error {
		c.newBackOff = fn
		return nil
	}
}

//WithConsumeTimeout time frame to wait for a new message.
// not setting this equals wait forever.
func WithConsumeTimeout(s string) (OptionFunc, error) {
	timeout, err := time.ParseDuration(s)
	if err != nil {
		return nil, err
	}

	return func(c *Config) error {
		c.consumeTimeout = timeout
		return nil
	}, nil
}

//WithMaxIntake ingest messages until maxIntake is reached.
func WithMaxIntake(num string) OptionFunc {
	return func(c *Config) error {
		n, err := strconv.Atoi(num)
		if err != nil {
			return err
		}
		c.maxIntake = n
		return nil
	}
}

//WithCloser adds an 'io.Closer' to the list.
func WithCloser(closer io.Closer) OptionFunc {
	return func(c *Config) error {
		c.closers = append(c.closers, closer)
		return nil
	}
}

// errorFunc will pass the initialization error through
func errorFunc(err error) OptionFunc {
	return func(c *Config) error {
		return err
	}
}

// DefaultEnvironment returns the optionFunc that expects 'RAVEN_URL', 'FLOW_ID' and 'WORKER_ID' as environmental variables
// 'CONSUME_TIMEOUT' will override the default if set. DefaultLogger is set as the logger.
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

	if s := os.Getenv("CONSUME_TIMEOUT"); s == "" {
	} else if optionFn, err := WithConsumeTimeout(s); err != nil {
		return errorFunc(err)
	} else {
		opts = append(opts, optionFn)
	}

	if optionFn, err := WithLogger(DefaultLogger); err != nil {
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

// CustomEnvironment returns the optionFunc and takes 'ravenURL', 'flowID' and 'workerID' as string arguments.
func CustomEnvironment(ravenURL, flowID, workerID string) OptionFunc {
	opts := []OptionFunc{}

	if optionFn, err := WithRavenURL((fmt.Sprintf("capnproto://%s", ravenURL))); err != nil {
		return errorFunc(err)
	} else {
		opts = append(opts, optionFn)
	}

	if optionFn, err := WithFlowID(flowID); err != nil {
		return errorFunc(err)
	} else {
		opts = append(opts, optionFn)
	}

	if optionFn, err := WithWorkerID(workerID); err != nil {
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
