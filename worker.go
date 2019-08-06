package ravenworker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"
)

const defaultHTTPTimeout = 5 * time.Second

type Config struct {
	RavenURL string
	WorkerId string
	FlowId   string
}

type Message struct {
	Ref     Reference
	Content string
}

type Reference struct {
	AckID   string `json:"ack_id"`
	EventId string `json:"event_id"`
	FlowId  string `json:"flow_id"`
}

// NewRavenworker returns a new configured raven worker client
func NewRavenworker(ravenURL, flowId, workerId string) (*Config, error) {
	if ravenURL == "" {
		return nil, errors.New("env RAVEN_URL needs to be set")
	}
	if flowId == "" {
		return nil, errors.New("env FLOW_ID needs to be set")
	}
	if workerId == "" {
		return nil, errors.New("env WORKER_ID needs to be set")
	}

	c := &Config{
		RavenURL: ravenURL,
		WorkerId: workerId,
		FlowId:   flowId,
	}
	return c, nil
}

// NewEvent sends a ravenworker Message. When a worker only creates
// new events/messages, use NewEvent.
// Use this function for the 'extract' worker type.
func (c Config) NewEvent(msg string) error {
	// Get unique event ID
	eventID, err := c.retrieveNewEventID()
	if err != nil {
		return err
	}

	m := Message{}
	m.Content = msg
	m.Ref.EventId = eventID

	// Put the new message
	u, err := url.Parse(c.RavenURL)
	if err != nil {
		return err
	}

	// Set path to correct endpoint
	u.Path = path.Join(u.Path, "flow", c.FlowId, "events", m.Ref.EventId)

	data, err := json.Marshal(m.Content)
	if err != nil {
		return err
	}

	// Create body from message
	var body io.Reader
	body = bytes.NewReader(data)

	// create the request
	req, err := http.NewRequest("PUT", u.String(), body)
	if err != nil {
		return err
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Response (AckJob) Status: %s, Error: %v",
			resp.Status, err)
	}
	return nil
}

// Consume gets a message from the queue. A Message is returned when there is
// a job, sleeps with incremented interval when there are no messages and
// returns an error in case of one.
// Use this function for the 'transform' and 'load' worker types.
func (c Config) Consume() (Message, error) {
	msg := Message{}
	noMsgCount := 0

	for {
		reference, err := c.getWork()
		if err != nil {
			if err != io.EOF {
				return msg, err
			}
			sleeper(noMsgCount)
			noMsgCount++
		} else {
			msg.Ref = reference
			break
		}
	}

	newEvent, err := c.getEvent(msg.Ref)
	if err != nil {
		return msg, err
	}

	msg.Content = newEvent
	return msg, nil
}

// Produce puts the Message with changed Message content and acks the message
// so it is removed from the queue.
// Use this function for the 'transform' worker type.
func (c Config) Produce(msg Message) error {
	// create new URL
	u, err := url.Parse(c.RavenURL)
	if err != nil {
		return err
	}

	// Set path to correct endpoint
	u.Path = path.Join(u.Path, "workers", c.WorkerId, "ack", msg.Ref.AckID)

	data, err := json.Marshal(msg.Content)
	if err != nil {
		return err
	}

	var msgBody io.Reader

	msgBody = bytes.NewReader(data)

	// create the request
	req, err := http.NewRequest("PUT", u.String(), msgBody)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{
		Timeout: defaultHTTPTimeout,
	}

	// Do the request
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Bad status returned. req: %s, Status: %s",
			u.String(), resp.Status)
	}
	return nil
}

func (c Config) getEvent(ref Reference) (string, error) {
	var msg string
	// Create endpoint
	u, err := url.Parse(c.RavenURL)
	if err != nil {
		return msg, err
	}

	// Set path to correct endpoint
	u.Path = path.Join(u.Path, "flow", c.FlowId, "events", ref.EventId)

	// create the request
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return msg, err
	}
	client := &http.Client{
		Timeout: defaultHTTPTimeout,
	}

	// Do the request
	resp, err := client.Do(req)
	if err != nil {
		return msg, err
	}
	defer resp.Body.Close()

	// New buffer to fill
	var body bytes.Buffer

	// body contains the message
	_, err = body.ReadFrom(resp.Body)
	if err != nil {
		return "", fmt.Errorf("Error reading response body: %s", err)
	}

	return body.String(), nil
}

// Check if there's work
func (c Config) getWork() (Reference, error) {
	// In the response is the reference to the message
	ref := Reference{}

	// Create endpoint
	u, err := url.Parse(c.RavenURL)
	if err != nil {
		return ref, err
	}

	// Set path to correct endpoint
	u.Path = path.Join(u.Path, "workers", c.WorkerId, "work")

	// create the request
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return ref, err
	}

	client := &http.Client{
		Timeout: defaultHTTPTimeout,
	}

	// Do the request
	resp, err := client.Do(req)
	if err != nil {
		return ref, nil
	}
	defer resp.Body.Close()

	// Decode into Reference
	err = json.NewDecoder(resp.Body).Decode(&ref)
	if err != nil {
		return ref, err
	}

	if resp.StatusCode != http.StatusOK {
		return ref, fmt.Errorf("Bad status returned. req: %s, Status: %s\n",
			u.String(), resp.Status)
	}

	fmt.Printf("AckID: %s\nEventID: %s\nFlowID: %s\n", ref.AckID,
		ref.EventId, ref.FlowId)
	return ref, nil
}

func (c Config) retrieveNewEventID() (string, error) {
	// create new URL
	u, err := url.Parse(c.RavenURL)
	if err != nil {
		return "", err
	}

	// Set path to correct endpoint
	u.Path = path.Join(u.Path, "flow", c.FlowId, "events")

	// create the request
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return "", err
	}

	client := &http.Client{
		Timeout: defaultHTTPTimeout,
	}

	// Do the request
	resp, err := client.Do(req)
	if err != nil {
		return "", nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad status returned. req: %s, Status: %s",
			u.String(), resp.Status)
	}

	// New buffer to fill
	var body bytes.Buffer
	// body is new event ID
	_, err = body.ReadFrom(resp.Body)
	if err != nil {
		return "", fmt.Errorf("Error reading response body: %s", err)
	}

	eventID := body.String()
	return eventID, nil
}

func sleeper(count int) {
	sleep := time.Duration(count * 1000 * 1000) // Duration in ns then ms then s
	if sleep > 5*time.Second {
		sleep = 5 * time.Second
	}
	fmt.Printf("Awaiting new messages. Interval %v miliseconds\n", sleep)
	time.Sleep(sleep * time.Millisecond)
}
