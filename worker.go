package worker

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/transcovo/go-chpr-logger"
	"github.com/transcovo/go-chpr-metrics"
)

var errDefaultRecover = errors.New("AMQP handler panicked")

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
	/*
		ChannelCloseTimeout is the duration to wait between channels cancelation
		and connection close.
		It lets handlers finish their tasks after a close is required.
		The higher it is, the more a task has time to finish its work
		By default, it will not wait
	*/
	ChannelCloseTimeout time.Duration

	// protects about data race condition on the channel
	mu                  sync.RWMutex
	requestCloseChannel chan chan struct{}
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
		ChannelCloseTimeout: 1*time.Second,
	}

	worker.Start(signals)

The AMQP consumers must all have a unique consumer tag. It can be provided in
the AmqpWorker given as parameter, or if it is an empty string, a random and
unique one is generated.
*/
func (worker *AmqpWorker) Start(signals <-chan os.Signal) {
	var channels []*amqp.Channel

	if worker.ConsumerTag == "" {
		data := consumerTagData{
			ProgramName: os.Args[0],
			UID:         rand.Int63(),
		}
		worker.ConsumerTag = uniqueConsumerTag(&data)
	}

	worker.mu.Lock()
	worker.requestCloseChannel = make(chan chan struct{})
	worker.mu.Unlock()

	conn, err := amqp.Dial(worker.AmqpURL)
	if err != nil {
		failOnError(err, "Failed to connect to RabbitMQ")
	}
	defer conn.Close()

	notifyClose := conn.NotifyClose(make(chan *amqp.Error))

	waitGroup := sync.WaitGroup{}
	for _, handler := range worker.Handlers {
		formattedQueueName := formatQueueName(worker.Queue, handler.RoutingKey)

		channel := getChannel(conn.Channel)

		declareExchange(channel.ExchangeDeclare, worker.Exchange)

		setChannelQos(channel.Qos, worker.ChannelPrefetchCount)

		declareQueue(channel.QueueDeclare, formattedQueueName, nil)

		bindQueue(channel.QueueBind, worker.Exchange, formattedQueueName, handler.RoutingKey)

		messages := consumeChannel(channel.Consume, formattedQueueName, worker.ConsumerTag)

		channels = append(channels, channel)
		waitGroup.Add(1)
		go handleMessages(messages, handler.Handler, &waitGroup)
	}

	requestClose := waitAnyEndSignal(signals, notifyClose, worker.requestCloseChannel)
	defer func() {
		if requestClose != nil {
			close(requestClose)
		}
	}()

	// after this call, messages channels will be closed soon
	for _, channel := range channels {
		channel.Cancel(worker.ConsumerTag, false)
	}
	waitGroup.Wait()
	// after we canceled the channels, we wait for a bit to let the handlers finish
	// their tasks
	time.Sleep(worker.ChannelCloseTimeout)
}

// Stop stops a running worker. If it is not running, it returns directly
func (worker *AmqpWorker) Stop() {
	worker.mu.RLock()
	requestCloseChan := worker.requestCloseChannel
	worker.mu.RUnlock()

	if requestCloseChan == nil {
		return
	}
	requestClose := make(chan struct{})
	requestCloseChan <- requestClose
	<-requestClose
}

/*
handleMessages call the handler on each amqp.Delivery = message
If an error on a correct message (ie not a validation error) is returned by the handler, then it is nacked and requeued
If an error on a correct message that has been redelivered ie failed before, the message is nacked to avoid queue filling up
If an error on a incorrect message is returned, the message is not requeued but nacked (this would cause the queue to fill up)
If no error is returned, the message is acked
*/
func handleMessages(messages <-chan amqp.Delivery, handler AmqpHandler, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	for msg := range messages {
		waitGroup.Add(1)
		go handleDeliveryWithRetry(msg, handler, waitGroup)
	}
}

/*
handleDeliveryWithRetry is meant to be called as a goroutine looped over a channel.
It wraps all the re-queuing mechanism.
*/
func handleDeliveryWithRetry(msg amqp.Delivery, handler AmqpHandler, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	err := handleSingleMessage(msg, handler)
	scopeLogger := logger.WithFields(logrus.Fields{
		"msg":     msg,
		"msgBody": string(msg.Body),
	})
	if err == nil {
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

	return handler.HandleMessage(msg.Body)
}

/*
waitAnyEndSignal allows the start function to receive any signal indicating a error in the worker and hence exiting safely
*/
func waitAnyEndSignal(signals <-chan os.Signal, amqpCloseConnection <-chan *amqp.Error, requestCloseChannel <-chan chan struct{}) chan struct{} {
	select {
	case signal := <-signals:
		logger.WithField("signal", signal).Info("[lib.amqp#waitAnyEndSignal] Received signal from OS")
		metrics.Increment("amqp.signals.os_exit")
		return nil
	case amqpError := <-amqpCloseConnection:
		logger.WithField("connectionError", amqpError).Error("[lib.amqp#waitAnyEndSignal] Received error from AMQP connection")
		metrics.Increment("amqp.signals.amqp_lost_connection")
		return nil
	case requestClose := <-requestCloseChannel:
		logger.Info("[lib.amqp#waitAnyEndSignal] Close requested")
		return requestClose
	}
}

/*
formatQueueName return the formatted queue name based on the base queue name and the routing key
*/
func formatQueueName(queueName string, routingKey string) string {
	return fmt.Sprintf("%s.%s", queueName, routingKey)
}
