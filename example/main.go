package main

import (
	"fmt"
	"os"
	"time"

	"go.dutchsec.com/ravenworker"
)

func main() {

	exampleExtract()
	// exampleTransform()
}

func exampleExtract() {
	c, _ := ravenworker.NewRavenworker(
		os.Getenv("RAVEN_URL"),
		os.Getenv("FLOW_ID"),
		os.Getenv("WORKER_ID"))

	message := "Some message to send "

	// Append time to message
	for i := 0; i < 10; i++ {
		message = message + time.Now().String()
		err := c.NewEvent(message)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func exampleTransform() {
	c, _ := ravenworker.NewRavenworker(
		os.Getenv("RAVEN_URL"),
		os.Getenv("FLOW_ID"),
		os.Getenv("WORKER_ID"))

	for {
		message, err := c.Consume()
		if err != nil {
			fmt.Println(err)
			continue
		}

		// Do something with the message
		message.Content = "This is the new message: " + c.WorkerId
		err = c.Produce(message)
		if err != nil {
			fmt.Printf("Error acknowledging message: %s", err)
		}
	}
}
