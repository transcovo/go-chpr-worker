package worker

import (
	"errors"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

const amqpURL = "amqp://guest:guest@localhost:5672"

func TestUniqueConsumerTag(t *testing.T) {
	data := &consumerTagData{
		ProgramName: "TestName",
		UID:         1234567890,
	}
	res := uniqueConsumerTag(data)
	assert.Equal(t, "ctag-TestName-1234567890", res)
}

func TestGetChannel_Success(t *testing.T) {
	channel := &amqp.Channel{}

	res := getChannel(func() (*amqp.Channel, error) {
		return channel, nil
	})

	assert.Equal(t, channel, res)
}

func TestGetChannel_Error(t *testing.T) {
	assert.Panics(t, func() {
		getChannel(func() (*amqp.Channel, error) {
			return nil, errors.New("some error")
		})
	})
}

func TestDeclareExchange_Success(t *testing.T) {
	assert.NotPanics(t, func() {
		declareExchange(func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
			if name != "exchange_name" ||
				kind != "topic" ||
				durable != true ||
				autoDelete != false ||
				internal != false ||
				noWait != false ||
				args != nil {
				return errors.New("invalid parameters")
			}
			return nil
		}, "exchange_name")
	})
}

func TestDeclareExchange_Error(t *testing.T) {
	assert.Panics(t, func() {
		declareExchange(func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
			return errors.New("some error")
		}, "exchange_name")
	})
}

func TestDeclareQueue_Success(t *testing.T) {
	queue := amqp.Queue{}

	res := declareQueue(func(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
		if name != "queue_name" ||
			durable != true ||
			autoDelete != false ||
			exclusive != false ||
			noWait != false ||
			args != nil {
			return queue, errors.New("invalid parameters")
		}
		return queue, nil
	}, "queue_name", nil)

	assert.Equal(t, queue, res)
}

func TestDeclareQueue_Error(t *testing.T) {
	queue := amqp.Queue{}
	assert.Panics(t, func() {
		declareQueue(func(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
			return queue, errors.New("some error")
		}, "queue_name", nil)
	})
}

func TestSetChannelQos_Success(t *testing.T) {
	assert.NotPanics(t, func() {
		setChannelQos(func(prefetchCount, prefetchSize int, global bool) error {
			if prefetchCount != 42 ||
				prefetchSize != 0 ||
				global != false {
				return errors.New("invalid parameters")
			}
			return nil
		}, 42)
	})
}

func TestSetChannelQos_Error(t *testing.T) {
	assert.Panics(t, func() {
		setChannelQos(func(int, int, bool) error {
			return errors.New("some error")
		}, 42)
	})
}

func TestBindQueue_Success(t *testing.T) {
	assert.NotPanics(t, func() {
		bindQueue(func(name, key, exchange string, noWait bool, args amqp.Table) error {
			if name != "queue_name" ||
				key != "routing_key" ||
				exchange != "exchange_name" ||
				noWait != false ||
				args != nil {
				return errors.New("invalid parameters")
			}
			return nil
		}, "exchange_name", "queue_name", "routing_key")
	})
}

func TestBindQueue_Error(t *testing.T) {
	assert.Panics(t, func() {
		bindQueue(func(name, key, exchange string, noWait bool, args amqp.Table) error {
			return errors.New("some error")
		}, "exchange_name", "queue_name", "routing_key")
	})
}

func TestConsumeChannel_Success(t *testing.T) {
	assert.NotPanics(t, func() {
		consumeChannel(func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			if queue != "queue_name" ||
				consumer != "consumer_tag" ||
				autoAck != false ||
				exclusive != false ||
				noLocal != false ||
				noWait != false ||
				args != nil {
				return nil, errors.New("invalid parameters")
			}
			return make(chan amqp.Delivery), nil
		}, "queue_name", "consumer_tag")
	})
}

func TestConsumeChannel_Error(t *testing.T) {
	assert.Panics(t, func() {
		consumeChannel(func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
			return nil, errors.New("some error")
		}, "queue_name", "consumer_tag")
	})
}

func purgeAMQPQueue(amqpURL, queueName string) {
	conn, _ := amqp.Dial(amqpURL)
	channel, _ := conn.Channel()
	channel.QueuePurge(queueName, false)
}

type testHandler struct {
	Error      error
	ResultChan chan string
}

func (handler *testHandler) HandleMessage(msg []byte) error {
	handler.ResultChan <- string(msg)
	return handler.Error
}

func publishAMQPTestMessage(url, exchange, routingKey string, message string) {
	conn, _ := amqp.Dial(url)
	channel, _ := conn.Channel()

	// XXX: wait for AMQP to start...
	time.Sleep(50 * time.Millisecond)
	channel.Publish(exchange, routingKey, false, false, amqp.Publishing{
		Body: []byte(message),
	})
}

func killWorkerAfter(closeChannel chan bool) {
	<-closeChannel
	time.Sleep(50 * time.Millisecond)
	process, _ := os.FindProcess(os.Getpid())
	process.Signal(syscall.SIGUSR1)
}

func TestStartAMQP_NoHandler(t *testing.T) {
	purgeAMQPQueue(amqpURL, "queue_name")

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGUSR1)

	closeChannel := make(chan bool)

	worker := AmqpWorker{
		AmqpURL:     amqpURL,
		Exchange:    "exchange_name",
		Queue:       "queue_name",
		ConsumerTag: "",
		Handlers:    []AmqpConsumer{},
	}

	go func() {
		// wait for worker to really start
		time.Sleep(50 * time.Millisecond)
		closeChannel <- true
	}()

	go killWorkerAfter(closeChannel)

	worker.Start(sigs)
}

var receivedSuccessMessages = make(chan string)

func TestStartAMQP_OneHandlerSuccess(t *testing.T) {
	key := "routing_key"

	purgeAMQPQueue(amqpURL, "queue_name")

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGUSR1)

	closeChannel := make(chan bool)

	worker := AmqpWorker{
		AmqpURL:     amqpURL,
		Exchange:    "exchange_name",
		Queue:       "queue_name",
		ConsumerTag: "",
		Handlers: []AmqpConsumer{{RoutingKey: key, Handler: &testHandler{
			Error:      nil,
			ResultChan: receivedSuccessMessages,
		}}},
	}

	go func() {
		// wait for the message handler
		body := <-receivedSuccessMessages
		assert.Equal(t, "test", body)

		closeChannel <- true
		<-receivedSuccessMessages
		assert.Fail(t, "Should not have received a message")
	}()

	go killWorkerAfter(closeChannel)

	go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, key, "test")

	worker.Start(sigs)
}

func TestStartAMQP_MultipleHandlersSuccess(t *testing.T) {
	var (
		keyOne              = "routing_key.one"
		keyTwo              = "routing_key.two"
		receivedHandlersOne = make(chan string)
		receivedHandlersTwo = make(chan string)
	)

	purgeAMQPQueue(amqpURL, "queue_name")

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGUSR1)

	closeChannel := make(chan bool)

	worker := AmqpWorker{
		AmqpURL:     amqpURL,
		Exchange:    "exchange_name",
		Queue:       "queue_name",
		ConsumerTag: "",
		Handlers: []AmqpConsumer{
			{
				RoutingKey: keyOne,
				Handler: &testHandler{
					Error:      nil,
					ResultChan: receivedHandlersOne,
				}},
			{
				RoutingKey: keyTwo,
				Handler: &testHandler{
					Error:      nil,
					ResultChan: receivedHandlersTwo,
				}},
		},
	}

	go func() {
		// wait for the message handler
		bodyOne := <-receivedHandlersOne
		bodyTwo := <-receivedHandlersTwo
		assert.Equal(t, "imthemessageforkeyone", bodyOne)
		assert.Equal(t, "imthemessageforkeytwo", bodyTwo)

		closeChannel <- true
		<-receivedHandlersOne
		<-receivedHandlersTwo
		assert.Fail(t, "Should not have received a message")
	}()

	go killWorkerAfter(closeChannel)

	go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, keyOne, "imthemessageforkeyone")
	go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, keyTwo, "imthemessageforkeytwo")

	worker.Start(sigs)
}

var receivedRedeliveredErrorMessages = make(chan string)

/*
TestStartAMQP_DropOnErrorOnRedeliveredMessage tests that in case of a redelivery resulting in a handling error, the message is not requeued
*/
func TestStartAMQP_DropOnErrorOnRedeliveredMessage(t *testing.T) {
	key := "routing_key"

	purgeAMQPQueue(amqpURL, "queue_name")

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGUSR1)

	closeChannel := make(chan bool)

	worker := AmqpWorker{
		AmqpURL:     amqpURL,
		Exchange:    "exchange_name",
		Queue:       "queue_name",
		ConsumerTag: "",
		Handlers: []AmqpConsumer{{RoutingKey: key, Handler: &testHandler{
			Error:      errors.New("Unknown error"), // an error that will trigger a requeue on first occurrence
			ResultChan: receivedRedeliveredErrorMessages,
		}}},
	}

	// Goroutine responsible for reading messages from handler function
	go func() {
		// wait for the message handler
		body := <-receivedRedeliveredErrorMessages
		assert.Equal(t, "test", body)
		body = <-receivedRedeliveredErrorMessages
		assert.Equal(t, "test", body)
		closeChannel <- true

		<-receivedRedeliveredErrorMessages
		assert.Fail(t, "Should not have received the message a third time")
	}()

	go killWorkerAfter(closeChannel)

	go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, key, "test")

	worker.Start(sigs)
}

var receivedRedeliveredPanicMessages = make(chan string)

type testPanicHandler struct {
	Error      error
	ResultChan chan string
}

func (handler *testPanicHandler) HandleMessage(msg []byte) error {
	handler.ResultChan <- string(msg)
	if handler.Error != nil {
		panic(handler.Error)
	}
	return nil
}

/*
TestStartAMQP_DropOnPanicOnRedeliveredMessage tests that in case of a panic, the message is redelivered and dropped the second time
*/
func TestStartAMQP_DropOnPanicOnRedeliveredMessage(t *testing.T) {
	key := "routing_key"

	purgeAMQPQueue(amqpURL, "queue_name")

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGUSR1)

	closeChannel := make(chan bool)

	worker := AmqpWorker{
		AmqpURL:     amqpURL,
		Exchange:    "exchange_name",
		Queue:       "queue_name",
		ConsumerTag: "",
		Handlers: []AmqpConsumer{{RoutingKey: key, Handler: &testPanicHandler{
			Error:      errors.New("some error"),
			ResultChan: receivedRedeliveredPanicMessages,
		}}},
	}

	go func() {
		body := <-receivedRedeliveredPanicMessages
		assert.Equal(t, "test", body)
		body = <-receivedRedeliveredPanicMessages
		assert.Equal(t, "test", body)
		closeChannel <- true

		<-receivedRedeliveredPanicMessages
		assert.Fail(t, "Should not have received the message a third time")
	}()

	go killWorkerAfter(closeChannel)

	go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, key, "test")

	worker.Start(sigs)
}

var receivedUnknownErrorMessages = make(chan string)

/*
TestStartAMQP_OneHandlerUnknownError tests the case of an unknown error handling the message, the message is redelivered once then nacked
*/
func TestStartAMQP_OneHandlerUnknownError(t *testing.T) {
	key := "routing_key"

	purgeAMQPQueue(amqpURL, "queue_name")

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGUSR1)

	closeChannel := make(chan bool)

	worker := AmqpWorker{
		AmqpURL:     amqpURL,
		Exchange:    "exchange_name",
		Queue:       "queue_name",
		ConsumerTag: "",
		Handlers: []AmqpConsumer{{RoutingKey: key, Handler: &testHandler{
			Error:      errors.New("random error"),
			ResultChan: receivedUnknownErrorMessages,
		}}},
	}

	go killWorkerAfter(closeChannel)

	go func() {
		// message is nacked and redelivered.
		body := <-receivedUnknownErrorMessages
		assert.Equal(t, "test", body)
		body = <-receivedUnknownErrorMessages
		assert.Equal(t, "test", body)

		closeChannel <- true

		<-receivedUnknownErrorMessages
		assert.Fail(t, "This message should not have been redelivered due to an unknown error")
	}()

	go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, key, "test")

	worker.Start(sigs)
}

/*
TestStartAMQP_AMQPError tests that the worker cannot start without a valid RabbitMQ URL
*/
func TestStartAMQP_AMQPError(t *testing.T) {
	purgeAMQPQueue(amqpURL, "queue_name")

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGUSR1)

	worker := AmqpWorker{
		AmqpURL:     "invalid_url",
		Exchange:    "exchange_name",
		Queue:       "queue_name",
		ConsumerTag: "",
		Handlers:    []AmqpConsumer{},
	}

	assert.Panics(t, func() {
		worker.Start(sigs)
	})
}

/*
TestWaitAnyEndSignal_HandlerEndSignal tests the reception in the endHandler channel
Considering that we need all handlers to exit correctly, we need to make sure that we also re inject into endHandler channel to match handlers count
*/
func TestWaitAnyEndSignal_HandlerEndSignal(t *testing.T) {
	handlerEnd := make(chan error)
	endConnection := make(chan int, 1)

	go func() {
		handlerEnd <- errors.New("Some handler went bust")
	}()
	go func() {
		handlerEnd <- errors.New("I am the second worker getting out of the line")
	}()

	waitAnyEndSignal(handlerEnd, nil, nil, endConnection, 2)
	endSignal := <-endConnection
	assert.Equal(t, 0, endSignal)
}

/*
TestwaitAnyEndSignal_Signals tests the reception in the signals channel
*/
func TestWaitAnyEndSignal_Signals(t *testing.T) {
	signals := make(chan os.Signal)
	handlerEnd := make(chan error)
	endConnection := make(chan int, 1)
	syncChan := make(chan int)

	go func() {
		signals <- nil
		time.Sleep(50 * time.Millisecond)
		syncChan <- 0
	}()
	go func() {
		<-syncChan
		handlerEnd <- errors.New("I am the handler exiting correctly")
		handlerEnd <- errors.New("I am the second handler getting out of the loop cleanly")
	}()

	waitAnyEndSignal(handlerEnd, signals, nil, endConnection, 2)

	endSignal := <-endConnection
	assert.Equal(t, 0, endSignal)
}

/*
TestwaitAnyEndSignal_AmqpCloseConnection tests the reception in the amqpCloseConnection channel
*/
func TestWaitAnyEndSignal_AmqpCloseConnection(t *testing.T) {
	amqpCloseConnection := make(chan *amqp.Error)
	handlerEnd := make(chan error)
	syncChan := make(chan int)

	go func() {
		amqpCloseConnection <- &amqp.Error{}
		time.Sleep(50 * time.Millisecond)
		syncChan <- 0
	}()
	go func() {
		<-syncChan
		handlerEnd <- errors.New("I am the handler getting out of the loop cleanly")
		handlerEnd <- errors.New("I am the second handler getting out of the loop cleanly")
	}()
	waitAnyEndSignal(handlerEnd, nil, amqpCloseConnection, nil, 2)
}

/*
TestFormatQueueName tests the formatting of the queue name
*/
func TestFormatQueueName(t *testing.T) {
	queueName := formatQueueName("worker.queue", "routing.key")

	assert.Equal(t, queueName, "worker.queue.routing.key")
}
