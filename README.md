# Raven-worker package

## Description
Raven-worker is used as the base of every `worker` in the Raven framework.  
A `worker` is a building block that can perform a specific task in a streaming data-flow.  
A data-stream that is configured from one or multiple workers, is called a `flow`.  
The goal of a `flow` is to filter, extract, enrich and store streaming data to easily manage all your (streaming) data.

## Worker types

Workers come in three different types: `extract`, `transform` and `load` workers. These types are based on the [graph theory](https://en.wikipedia.org/wiki/Graph_theory) where each worker represents a `node`.  
Consider any Raven flow a `directed acyclic graph`.  


## About Workers

* Workers need to be as simple as possible, only do their job. Everything else
  will be handled by Raven.
* If an error occurs, just panic. Raven will handle monitoring and restarting
  workers. The Raven Worker base package will use a sequential backoff algorithm for
  handling incidental errors.
* If there isn't enough work, for a longer period, just stop. Raven will monitor
  backlogs and start new workers when necessary. 

## How to use

The following functions are exposed by the package:

### New
This initializes the worker.  

For the Raven framework, a `worker` needs three variables to work with:
* `ravenworker.WithRavenURL` to connect to the Raven framework. Multiple values
  can be used here, as it will use them in a round robin configuration.  
* `ravenworker.WithWorkerID` to identify itself and get new jobs.  
* `ravenworker.WithFlowID` to get the right jobs for the flow it belongs to and therefore receive the right events (messages) to process.  

The `ravenworker.DefaultEnvironment()` method can be used for convenience, to
load the configuration from environment variables.

Example:
```go
	c, err := ravenworker.New(
        ravenworker.DefaultEnvironment(),
    )

    if err != nil {
        // handle error
    }
```


Now you can use the methods:

### Consume
When a worker is of type `transform` or `load`, use `Consume` to retrieve the message from the stream.  

Example:
```go
    ref, err := c.Consume()
    if err != nil {
        // handle error
    }
```

### Get
Get will retrieve the actual message.

Example:
```go
    msg, err := c.Get(ref)
    if err != nil {
        // handle error
    }
```

### Ack
Ack will acknowledge the message and proceeds the flow.

Example:
```go
    msg, err := c.Ack(ref, WithMessage(msg), WithFilter())
    if err != nil {
        // handle error
    }
```

### Produce
When a worker is of type `transform` or `load`, use `Produce` to put the new message or ack the message.  
The actual content (payload) is stored in `message.Content` which takes a byte
array. Convenience functions like JsonContent (which encodes the object to json
byte array) exists.

Example:
```go
    message := NewMessage()
    message.Content = JsonContent(obj)

    if err := c.Produce(message); err != nil {
        // handle error
    }
```

