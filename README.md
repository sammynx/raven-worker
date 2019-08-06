# Raven-worker package

## Description
Raven-worker is used as the base of every `worker` in the Raven framework.  
A `worker` is a building block that can perform a specific task in a streaming data-flow.  
A data-stream that is configured from one or multiple workers, is called a `flow`.  
The goal of a `flow` is to filter, extract, enrich and store streaming data to easily manage all your (streaming) data.

## Worker types

Workers come in three different types: `extract`, `transform` and `load` workers. These types are based on the [graph theory](https://en.wikipedia.org/wiki/Graph_theory) where each worker represents a `node`.  
For Raven we only use a `directed acyclic graph`.  

## How to use

The following functions are exposed by the package:

### NewRavenworker
This initializes the worker.  

For the Raven framework, a `worker` needs three variables to work with:
* `RavenURL` to connect to the Raven framework.  
* `WorkerID` to identify itself and get new jobs.  
* `FlowID` to get the right jobs for the flow it belongs to and therefor receive the right events (messages) to process.  

Example:
```go
	c, _ := ravenworker.NewRavenworker(
		os.Getenv("RAVEN_URL"),
		os.Getenv("FLOW_ID"),
		os.Getenv("WORKER_ID"))

```

Now you can use the methods:

### NewEvent
If the worker is of type `extract` which generates data or gets data from an external source, use the `NewEvent` method.  
This method will generate new events for the flow.  

Example:
```go
	message := "Some message to send "
    err := c.NewEvent(message)
    if err != nil {
        fmt.Println(err)
    }
```

### Consume
When a worker is of type `transform` or `load`, use `Consume` to retrieve the message from the stream.  

Example:
```go
    message, err := c.Consume()
    if err != nil {
        // handle err
    }
    // do something with the message
```


### Produce
When a worker is of type `transform` or `load`, use `Produce` to put the new message or ack the message.  
The actual content (payload) is stored in `message.Content` which takes a string.  

Example:
```go
		message.Content = "This is the new message: "
		err = c.Produce(message)
		if err != nil {
            // handle err
		}
```
