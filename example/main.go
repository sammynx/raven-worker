package main

import (
	"fmt"
	"time"

	ravenworker "github.com/dutchsec/raven-worker"
	worker "github.com/dutchsec/raven-worker"
)

var log = worker.DefaultLogger

func main() {
	exampleProduce()

	exampleTransform()
}

// exampleProduce will just produce a new message
func exampleProduce() {
	c, err := worker.New(
		worker.DefaultEnvironment(),
	)
	if err != nil {
		log.Fatalf("Could not initialize raven worker: %s", err)
	}

	for i := 0; i < 10; i++ {
		message := worker.NewMessage()

		message = message.Content([]byte(fmt.Sprintf("message %v", time.Now().String())))

		if err := c.Produce(message); err != nil {
			log.Fatalf("Could not produce events: %s", err)
		}
	}
}

// exampleTransform will consume a new message, update the message
// and acknowledge the message with the new content
func exampleTransform() {
	c, err := worker.New(
		worker.DefaultEnvironment(),
	)
	if err != nil {
		log.Fatalf("Could not initialize raven worker: %s", err)
	}

	for {
		ref, err := c.Consume()
		if err != nil {
			log.Fatalf("Could not consume message: %s\n", err)
		}

		message, err := c.Get(ref)
		if err != nil {
			log.Fatalf("Could not consume message: %s\n", err)
		}

		newMessage := message.Content(
			worker.StringContent(
				fmt.Sprintf("This is the new message: %s", time.Now().String()),
			),
		)

		if err := c.Ack(ref, ravenworker.WithMessage(newMessage)); err != nil {
			log.Fatalf("Could not ack message: %s\n", err)
		}
	}
}
