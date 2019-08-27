# Raven-worker package

## Description
Raven-worker is used as the base of every `worker` in the Raven framework.  
A `worker` is a building block that can perform a specific task in a streaming data-flow.  
A data-stream that is configured from one or multiple workers, is called a `flow`.  
The goal of a `flow` is to filter, extract, enrich and store streaming data to easily manage all your (streaming) data.

## Worker types

Workers come in three different types: `extract`, `transform` and `load` workers. These types are based on the [graph theory](https://en.wikipedia.org/wiki/Graph_theory) where each worker represents a `node`.  
Consider any Raven flow a `directed acyclic graph`.  

## How to use

The following functions are exposed by the package:

### NewRavenworker
This initializes the worker.  

For the Raven framework, a `worker` needs three variables to work with:
* `RavenURL` to connect to the Raven framework.  
* `WorkerID` to identify itself and get new jobs.  
* `FlowID` to get the right jobs for the flow it belongs to and therefore receive the right events (messages) to process.  

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

    msg, err := c.Get(ref)
    if err != nil {
        // handle error
    }

    // do something with the message
    msg, err := c.Ack(ref, WithMessage(msg), WithFilter())
    if err != nil {
        // handle error
    }
```


### Produce
When a worker is of type `transform` or `load`, use `Produce` to put the new message or ack the message.  
The actual content (payload) is stored in `message.Content` which takes a string.  

Example:
```go
    message := NewMessage().
                    Content(StringContent("This is the new message: "))

    err := c.Produce(message)
    if err != nil {
        // handle error
    }
```

