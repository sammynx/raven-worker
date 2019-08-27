package main

import (
	"fmt"
	"time"

	worker "github.com/dutchsec/raven-worker"
)

var log = worker.DefaultLogger

func main() {
	exampleExtract()
	exampleTransform()
}

func exampleExtract() {
	// TODO: change OptionFunc, error
	c, err := worker.New(worker.DefaultEnvironment())
	if err != nil {
		log.Fatalf("Could not initialize raven worker: %s", err)
	}

	// Append time to message
	for i := 0; i < 10; i++ {
		message := worker.NewMessage()

		message = message.Content([]byte(fmt.Sprintf("message %v", time.Now().String())))

		if err := c.Produce(message); err != nil {
			log.Fatalf("Could not produce events: %s", err)
		}
	}
}

// TODO: Message vs Ref vs Content vs Metadata is inconsistent currently
func exampleTransform() {
	c, err := worker.New(worker.DefaultEnvironment())
	if err != nil {
		log.Fatalf("Could not initialize raven worker: %s", err)
	}

	for {
		message, err := c.Consume()
		if err != nil {
			log.Fatalf("Could not consume message: %s\n", err)
		}

		// Do something with the message
		message = message.Content([]byte(fmt.Sprintf("This is the new message: %s", c.WorkerID)))

		if err := c.Ack(message); err != nil {
			log.Fatalf("Could not ack message: %s\n", err)
		}
	}
}
