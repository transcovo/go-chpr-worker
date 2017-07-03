This library packages everything you need to implement a worker that
takes tasks from AMQP messages.

## Example

The example below borrowed to pidget shows how to use it to implement a typical
usecase:

```go
// listen to os Signals to allow a graceful shutdown
sigs := make(chan os.Signal)
signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

// create the worker with the desired configuration
// note: you really don't have to use viper :=)
w := worker.AmqpWorker{
    AmqpURL:              viper.GetString("amqp.url"),
    Exchange:             viper.GetString("amqp.common.exchange"),
    Queue:                viper.GetString("amqp.sendWorker.queue"),
    ChannelPrefetchCount: viper.GetInt("send_worker.amqpPrefetchCount"),
    ConsumerTag:          "",
    Handlers:             getAmqpHandlers(db),
}

// start the worker
w.Start(sigs)
```
