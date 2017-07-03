package worker

import (
	"errors"
	"fmt"
	"math/rand"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/transcovo/go-chpr-logger"
	"github.com/transcovo/go-chpr-metrics"
)

var errDefaultRecover = errors.New("AMQP handler panicked")

/*
AmqpPublisher defines the methods an AMQP publisher must define.
*/
type AmqpPublisher interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

/*
AmqpHandler interface represents a AMQP message handler.
*/
type AmqpHandler interface {
	HandleMessage([]byte) error
}

/*
AmqpConsumer struct declares a couple of AMQP routing key and AmqpHandlerFunc to
call when a message is received.
*/
type AmqpConsumer struct {
	/*
		RoutingKey is the AMQP routing key on which to bind the queue.
	*/
	RoutingKey string
	/*
		Handler is the function to call when a message matching the RoutingKey is received.
		It should return an error when there is a need to re-queue the message for a second try.
	*/
	Handler AmqpHandler
}

/*
AmqpWorker struct contains the necessary information to initialize, connect
and consume messages on AMQP.
*/
type AmqpWorker struct {
	/*
		AmqpURL is the AMQP connection string.
	*/
	AmqpURL string
	/*
		Exchange is the AMQP exchange name.
	*/
	Exchange string
	/*
		Queue is the AMQP queue name.
	*/
	Queue string
	/*
		ConsumerTag is the AMQP consumer tag.

		It must be unique per consumer. If an empty string is given, a random and
		unique one will be generated.
	*/
	ConsumerTag string
	/*
		ChannelPrefetchCount is the AMQP prefetch count setting.

		It can be set to 0 to disable the setting.
	*/
	ChannelPrefetchCount int
	/*
		Handlers is an array of AmqpConsumer to consume on AMQP.
	*/
	Handlers []AmqpConsumer
}

type consumerTagData struct {
	ProgramName string
	UID         int64
}

func uniqueConsumerTag(data *consumerTagData) string {
	return fmt.Sprintf("ctag-%s-%d", data.ProgramName, data.UID)
}

func failOnError(err error, msg string) {
	if err != nil {
		logger.WithFields(logrus.Fields{
			"msg": msg,
			"err": err,
		}).Error("[lib.amqp#failOnError] Error")
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func getChannel(channelGetter func() (*amqp.Channel, error)) *amqp.Channel {
	channel, err := channelGetter()
	if err != nil {
		failOnError(err, "Failed to open a channel")
	}
	return channel
}

type exchangeDeclareFunc func(string, string, bool, bool, bool, bool, amqp.Table) error

func declareExchange(exchangeDeclare exchangeDeclareFunc, exchangeName string) {
	err := exchangeDeclare(exchangeName, amqp.ExchangeTopic, true, false, false, false, nil)
	if err != nil {
		failOnError(err, "Failed to declare an exchange")
	}
}

type queueDeclareFunc func(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error)

func declareQueue(queueDeclare queueDeclareFunc, queueName string, args amqp.Table) amqp.Queue {
	queue, err := queueDeclare(queueName, true, false, false, false, args)
	if err != nil {
		failOnError(err, "Failed to declare a queue")
	}
	return queue
}

type channelQosFunc func(int, int, bool) error

func setChannelQos(qosSet channelQosFunc, prefetchCount int) {
	err := qosSet(prefetchCount, 0, false)
	if err != nil {
		failOnError(err, "Failed to set channel QoS")
	}
}

type queueBindFunc func(string, string, string, bool, amqp.Table) error

func bindQueue(queueBind queueBindFunc, exchangeName, queueName, routingKey string) {
	err := queueBind(queueName, routingKey, exchangeName, false, nil)
	if err != nil {
		failOnError(err, "Failed to declare bind a queue")
	}
}

type consumeChannelFunc func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error)

func consumeChannel(consume consumeChannelFunc, queueName, consumerTag string) <-chan amqp.Delivery {
	messages, err := consume(queueName, consumerTag, false, false, false, false, nil)
	if err != nil {
		failOnError(err, "Failed to declare consume a queue")
	}
	return messages
}

/*
Start function is a helper to connect, initialize and consume messages on
AMQP.

The steps are the following:
* connect to AMQP
* get the channel
* declare the exchange
* declare the queue
* set the QoS settings (prefetch count)
* for the array of handlers bind and consume the queue

The function is blocking, is waits for system signals to close the connection
and return.

The caller has to declare a configuration structure (AmqpWorker) and define
a channel to listen on system signals:

	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	worker := lib.AmqpWorker{
		AmqpURL:              "amqp://localhost",
		Exchange:             "exchange_name",
		Queue:                "queue_name",
		ChannelPrefetchCount: 0,
		ConsumerTag:          "",
		Handlers:             []lib.AmqpConsumer{lib.AmqpConsumer{RoutingKey: "routing_key", Handler: someHandler}},
	}

	worker.Start(signals)

The AMQP consumers must all have a unique consumer tag. It can be provided in
the AmqpWorker given as parameter, or if it is an empty string, a random and
unique one is generated.
*/
func (worker *AmqpWorker) Start(signals <-chan os.Signal) {
	if worker.ConsumerTag == "" {
		data := consumerTagData{
			ProgramName: os.Args[0],
			UID:         rand.Int63(),
		}
		worker.ConsumerTag = uniqueConsumerTag(&data)
	}

	conn, err := amqp.Dial(worker.AmqpURL)
	if err != nil {
		failOnError(err, "Failed to connect to RabbitMQ")
	}

	notifyClose := conn.NotifyClose(make(chan *amqp.Error))

	handlerEnd := make(chan error, len(worker.Handlers))

	channel := getChannel(conn.Channel)

	declareExchange(channel.ExchangeDeclare, worker.Exchange)

	queue := declareQueue(channel.QueueDeclare, worker.Queue, nil)

	setChannelQos(channel.Qos, worker.ChannelPrefetchCount)

	for _, handler := range worker.Handlers {
		bindQueue(channel.QueueBind, worker.Exchange, queue.Name, handler.RoutingKey)

		messages := consumeChannel(channel.Consume, worker.Queue, worker.ConsumerTag)
		go handleMessages(messages, handler.Handler, handlerEnd)
	}

	endConnection := make(chan int)

	go func() {
		defer conn.Close()
		<-endConnection
		channel.Cancel(worker.ConsumerTag, false)
	}()
	waitAnyEndSignal(handlerEnd, signals, notifyClose, endConnection, len(worker.Handlers))
}

/*
handleMessages call the handler on each amqp.Delivery = message
If an error on a correct message (ie not a validation error) is returned by the handler, then it is nacked and requeued
If an error on a correct message that has been redelivered ie failed before, the message is nacked to avoid queue filling up
If an error on a incorrect message is returned, the message is not requeued but nacked (this would cause the queue to fill up)
If no error is returned, the message is acked
*/
func handleMessages(messages <-chan amqp.Delivery, handler AmqpHandler, done chan error) {
	for msg := range messages {
		go handleDeliveryWithRetry(msg, handler)
	}

	logger.Info("[lib.amqp#handleMessages] Out of loop, done")
	done <- nil
}

/*
handleDeliveryWithRetry is meant to be called as a goroutine looped over a channel.
It wraps all the re-queuing mechanism.
*/
func handleDeliveryWithRetry(msg amqp.Delivery, handler AmqpHandler) {
	err := handleSingleMessage(msg, handler)
	scopeLogger := logger.WithFields(logrus.Fields{
		"msg":     msg,
		"msgBody": string(msg.Body),
	})
	if err == nil {
		metrics.Increment("amqp.msg.ack")
		scopeLogger.Info("[lib.amqp#handleDeliveryWithRetry] Acking message")
		msg.Ack(false)
		return
	}
	scopeLogger = scopeLogger.WithError(err)
	requeue := !msg.Redelivered
	if requeue {
		metrics.Increment("amqp.msg.nack.requeue")
		scopeLogger.Warn("[lib.amqp#handleDeliveryWithRetry] First error handling on a message, requeuing it")
	} else {
		metrics.Increment("amqp.msg.nack.no_requeue")
		scopeLogger.Error("[lib.amqp#handleDeliveryWithRetry] Second handling error on a message, nacking it")
	}
	msg.Nack(false, requeue)
}

/*
handleSingleMessage calls the given handler with the given message and catches panics.

On panic, the error is returned, otherwise the handler result is returned.

To be able to return the panic result, we used a named return variable to be able to set it in the
defer after the recover.
*/
func handleSingleMessage(msg amqp.Delivery, handler AmqpHandler) (err error) {
	defer func() {
		recoverErr := recover()
		if recoverErr == nil {
			return
		}

		logger.WithFields(logrus.Fields{
			"err": recoverErr,
			"msg": msg,
		}).Error("[AMQP Recovery] Recovered panic from handler")

		err = errDefaultRecover
		castedErr, ok := recoverErr.(error)
		if ok {
			err = castedErr
		}
	}()

	err = handler.HandleMessage(msg.Body)
	return
}

/*
waitForHandlers blocks the reception
*/
func waitForHandlers(done chan error, handlersCount int) {
	logger.Info("[lib.amqp#waitForHandlers] Shutdown, waiting for handlers to terminate")

	for i := 0; i < handlersCount; i++ {
		<-done
	}
	logger.Info("[lib.amqp#waitForHandlers] All handlers terminated")
}

/*
waitAnyEndSignal allows the start function to receive any signal indicating a error in the worker and hence exiting safely
*/
func waitAnyEndSignal(handlerEnd chan error, signals <-chan os.Signal, amqpCloseConnection chan *amqp.Error, endConnection chan int, handlerCount int) {
	select {
	case <-handlerEnd:
		logger.Warning("[lib.amqp#waitAnyEndSignal] Received end signal from handleMessage function")
		metrics.Increment("amqp.signals.handler_exited")
		handlerCount--
		endConnection <- 0
	case signal := <-signals:
		logger.WithField("signal", signal).Info("[lib.amqp#waitAnyEndSignal] Received signal from OS")
		metrics.Increment("amqp.signals.os_exit")
		endConnection <- 0
	case amqpError := <-amqpCloseConnection:
		logger.WithField("connectionError", amqpError).Error("[lib.amqp#waitAnyEndSignal] Received error from AMQP connection")
		metrics.Increment("amqp.signals.amqp_lost_connection")
	}
	waitForHandlers(handlerEnd, handlerCount)
}
