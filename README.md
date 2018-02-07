[![CircleCI](https://circleci.com/gh/transcovo/go-chpr-worker.svg?style=shield)](https://circleci.com/gh/transcovo/go-chpr-worker)
[![codecov](https://codecov.io/gh/transcovo/go-chpr-worker/branch/master/graph/badge.svg)](https://codecov.io/gh/transcovo/go-chpr-worker)


This library packages everything you need to implement a worker that takes tasks
from AMQP messages.

## Example

The example below shows how to implement a typical usecase:

```go
// listen to os Signals to allow a graceful shutdown
sigs := make(chan os.Signal)
signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

// create the worker with the desired configuration
w := worker.AmqpWorker{
		AmqpURL:              "amqp://guest:guest@localhost:5672",
		Exchange:             "bus",
		Queue:                "application.worker",
		ChannelPrefetchCount: 100,
		ConsumerTag:          "",
		Handlers: []worker.AmqpConsumer{
			{
				RoutingKey: "routing.key",
				Handler:    myHandler,
			},
		},
	}

// start the worker
w.Start(sigs)
```

### Notes

The `Queue` field of the `AmqpWorker` struct must be unique for each worker to
avoid misrouting messages to the wrong `AmqpConsumer`.

We generate a queue name using both `Queue` and the `RoutingKey` for
each `AmqpConsumer`.

So if we take the previous example the generated queue name will be
`application.worker.routing.key`
