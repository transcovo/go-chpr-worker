package worker

import (
	"errors"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"
)

const amqpURL = "amqp://guest:guest@localhost:5672"

func purgeAMQPQueue(amqpURL, queueName string) {
	conn, _ := amqp.Dial(amqpURL)
	channel, _ := conn.Channel()
	channel.QueuePurge(queueName, false)
}

func waitABit() {
	time.Sleep(50 * time.Millisecond)
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
	waitABit()
	channel.Publish(exchange, routingKey, false, false, amqp.Publishing{
		Body: []byte(message),
	})
}

func killWorkerAfter(closeChannel chan bool) {
	<-closeChannel
	waitABit()
	killWorkerDirectly()
}

func killWorkerDirectly() {
	process, _ := os.FindProcess(os.Getpid())
	process.Signal(syscall.SIGUSR1)
}

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

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Worker Suite")
}

var _ = Describe("Worker", func() {
	Describe("#uniqueConsumerTag()", func() {
		It("should return an unique consumer tag", func() {
			data := &consumerTagData{
				ProgramName: "TestName",
				UID:         1234567890,
			}
			res := uniqueConsumerTag(data)
			Expect(res).To(Equal("ctag-TestName-1234567890"))
		})
	})

	Describe("#getChannel()", func() {
		It("should return channel in case of success", func() {
			channel := &amqp.Channel{}

			res := getChannel(func() (*amqp.Channel, error) {
				return channel, nil
			})
			Expect(res).To(Equal(channel))
		})

		It("should return error in case of error", func() {
			fn := func() {
				getChannel(func() (*amqp.Channel, error) {
					return nil, errors.New("some error")
				})
			}

			Expect(fn).To(Panic())
		})
	})

	Describe("#declareExchange()", func() {
		It("should declare exchange and not return error", func() {
			var name, kind string
			var durable, autoDelete, internal, noWait bool
			var args amqp.Table

			fn := func() {
				declareExchange(func(pname, pkind string, pdurable, pautoDelete, pinternal, pnoWait bool, pargs amqp.Table) error {
					name = pname
					kind = pkind
					durable = pdurable
					autoDelete = pautoDelete
					internal = pinternal
					noWait = pnoWait
					args = pargs
					return nil
				}, "exchange_name")
			}

			Expect(fn).ToNot(Panic())

			Expect(name).To(Equal("exchange_name"))
			Expect(kind).To(Equal("topic"))
			Expect(durable).To(Equal(true))
			Expect(autoDelete).To(Equal(false))
			Expect(internal).To(Equal(false))
			Expect(noWait).To(Equal(false))
			Expect(args).To(BeNil())
		})

		It("should panic if declare exchange fails", func() {
			fn := func() {
				declareExchange(func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
					return errors.New("some error")
				}, "exchange_name")
			}

			Expect(fn).To(Panic())
		})
	})

	Describe("#declareQueue()", func() {
		It("should return declared queue", func() {
			var name string
			var durable, autoDelete, exclusive, noWait bool
			var args amqp.Table

			queue := amqp.Queue{}

			res := declareQueue(func(pname string, pdurable, pautoDelete, pexclusive, pnoWait bool, pargs amqp.Table) (amqp.Queue, error) {
				name = pname
				durable = pdurable
				autoDelete = pautoDelete
				exclusive = pexclusive
				noWait = pnoWait
				args = pargs
				return queue, nil
			}, "queue_name", nil)

			Expect(res).To(Equal(queue))
			Expect(name).To(Equal("queue_name"))
			Expect(durable).To(Equal(true))
			Expect(exclusive).To(Equal(false))
			Expect(noWait).To(Equal(false))
			Expect(args).To(BeNil())
		})

		It("should panic if declare queue fails", func() {
			fn := func() {
				declareQueue(func(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
					return amqp.Queue{}, errors.New("some error")
				}, "queue_name", nil)
			}

			Expect(fn).To(Panic())
		})
	})

	Describe("setChannelQos()", func() {
		It("should set channel qos", func() {
			var prefetchCount, prefetchSize int
			var global bool

			setChannelQos(func(pprefetchCount, pprefetchSize int, pglobal bool) error {
				prefetchCount = pprefetchCount
				prefetchSize = pprefetchSize
				global = pglobal
				return nil
			}, 42)

			Expect(prefetchCount).To(Equal(42))
			Expect(prefetchSize).To(Equal(0))
			Expect(global).To(Equal(false))
		})

		It("should panic if set channel qos fails", func() {
			fn := func() {
				setChannelQos(func(int, int, bool) error {
					return errors.New("some error")
				}, 42)
			}

			Expect(fn).To(Panic())
		})
	})

	Describe("#bindQueue()", func() {
		It("should bind queue", func() {
			var name, key, exchange string
			var noWait bool
			var args amqp.Table

			bindQueue(func(pname, pkey, pexchange string, pnoWait bool, pargs amqp.Table) error {
				name = pname
				key = pkey
				exchange = pexchange
				noWait = pnoWait
				args = pargs
				return nil
			}, "exchange_name", "queue_name", "routing_key")

			Expect(name).To(Equal("queue_name"))
			Expect(key).To(Equal("routing_key"))
			Expect(noWait).To(Equal(false))
			Expect(args).To(BeNil())
		})

		It("should panic if bind queue fails", func() {
			fn := func() {
				bindQueue(func(name, key, exchange string, noWait bool, args amqp.Table) error {
					return errors.New("some error")
				}, "exchange_name", "queue_name", "routing_key")
			}

			Expect(fn).To(Panic())
		})
	})

	Describe("#consumeChannel()", func() {
		It("should consume channel", func() {
			var queue, consumer string
			var autoAck, exclusive, noLocal, noWait bool
			var args amqp.Table

			res := consumeChannel(func(pqueue, pconsumer string, pautoAck, pexclusive, pnoLocal, pnoWait bool, pargs amqp.Table) (<-chan amqp.Delivery, error) {
				queue = pqueue
				consumer = pconsumer
				autoAck = pautoAck
				exclusive = pexclusive
				noLocal = pnoLocal
				noWait = pnoWait
				args = pargs
				return make(chan amqp.Delivery), nil
			}, "queue_name", "consumer_tag")

			Expect(res).ToNot(BeNil())
			Expect(res).ToNot(BeClosed())
			Expect(queue).To(Equal("queue_name"))
			Expect(consumer).To(Equal("consumer_tag"))
			Expect(autoAck).To(Equal(false))
			Expect(exclusive).To(Equal(false))
			Expect(noLocal).To(Equal(false))
			Expect(noWait).To(Equal(false))
			Expect(args).To(BeNil())
		})

		It("should panic if consume channel fails", func() {
			fn := func() {
				consumeChannel(func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
					return nil, errors.New("some error")
				}, "queue_name", "consumer_tag")
			}

			Expect(fn).To(Panic())
		})
	})

	Describe("#Start()", func() {
		It("should start with no handler", func() {
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
				waitABit()
				closeChannel <- true
			}()

			go killWorkerAfter(closeChannel)

			worker.Start(sigs)
		})

		It("should call handler with received message", func() {
			receivedSuccessMessages := make(chan string)
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
				closeChannel <- true

				Expect(body).To(Equal("test"))
			}()

			go killWorkerAfter(closeChannel)

			go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, key, "test")

			worker.Start(sigs)
		})

		It("should redeliver message one more time if it fails then drop message", func() {
			var receivedRedeliveredErrorMessages = make(chan string)
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
				redeliveredBody := <-receivedRedeliveredErrorMessages
				closeChannel <- true

				Expect(body).To(Equal("test"))
				Expect(redeliveredBody).To(Equal("test"))
			}()

			go killWorkerAfter(closeChannel)

			go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, key, "test")

			worker.Start(sigs)

		})

		It("should redeliver message one more time if there is a panic", func() {
			var receivedRedeliveredPanicMessages = make(chan string)
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
				redeliveredBody := <-receivedRedeliveredPanicMessages
				closeChannel <- true

				Expect(body).To(Equal("test"))
				Expect(redeliveredBody).To(Equal("test"))
			}()

			go killWorkerAfter(closeChannel)

			go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, key, "test")

			worker.Start(sigs)
		})

		It("should redeliver then nack message in case of unknown error", func() {
			var receivedUnknownErrorMessages = make(chan string)
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
				redeliveredBody := <-receivedUnknownErrorMessages

				closeChannel <- true

				Expect(body).To(Equal("test"))
				Expect(redeliveredBody).To(Equal("test"))
			}()

			go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, key, "test")

			worker.Start(sigs)
		})

		It("should correctly start with two handlers and routing key'", func() {
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

				closeChannel <- true
				Expect(bodyOne).To(Equal("imthemessageforkeyone"))
				Expect(bodyTwo).To(Equal("imthemessageforkeytwo"))
			}()

			go killWorkerAfter(closeChannel)

			go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, keyOne, "imthemessageforkeyone")
			go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, keyTwo, "imthemessageforkeytwo")

			worker.Start(sigs)
		})

		It("should return an error then nack for unknown error", func() {
			var receivedUnknownErrorMessages = make(chan string)
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
				redeliveredBody := <-receivedUnknownErrorMessages

				closeChannel <- true

				Expect(body).To(Equal("test"))
				Expect(redeliveredBody).To(Equal("test"))
			}()

			go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, key, "test")

			worker.Start(sigs)
		})

		It("should not wait too long before closing", func() {
			routingKey := "routing_key"
			receivedHandler := make(chan string)

			purgeAMQPQueue(amqpURL, "queue_name")

			sigs := make(chan os.Signal)
			signal.Notify(sigs, syscall.SIGUSR1)

			raceConditionChannel := make(chan struct{}, 1)
			closeChannel := make(chan struct{})

			worker := AmqpWorker{
				AmqpURL:     amqpURL,
				Exchange:    "exchange_name",
				Queue:       "queue_name",
				ConsumerTag: "",
				Handlers: []AmqpConsumer{
					{
						RoutingKey: routingKey,
						Handler: &testHandler{
							Error:      nil,
							ResultChan: receivedHandler,
						},
					},
				},
				ChannelCloseTimeout: 50 * time.Millisecond,
			}

			go func() {
				// wait for the message handler
				<-receivedHandler
				// make the main goroutine to stop the worker
				close(closeChannel)
				// make it a bit faster than the timeout
				// so it will close the channel before returning from start
				time.Sleep(40 * time.Millisecond)
				raceConditionChannel <- struct{}{}
			}()

			go func() {
				<-closeChannel
				killWorkerDirectly()
			}()
			// this should still be open

			go publishAMQPTestMessage(worker.AmqpURL, worker.Exchange, routingKey, "test")

			worker.Start(sigs)

			// the handler should have had time to send its message
			_, hasReceivedMessage := <-raceConditionChannel
			Expect(hasReceivedMessage).To(BeTrue())
		})

		It("should return panic if start fails", func() {
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

			fn := func() {
				worker.Start(sigs)
			}
			Expect(fn).To(Panic())
		})
	})

	Describe("#waitAnyEndSignal()", func() {
		It("should return when message is received on amqpCloseConnection", func() {
			sigs := make(chan os.Signal)
			amqpCloseConnection := make(chan *amqp.Error, 1)

			amqpCloseConnection <- &amqp.Error{}
			waitAnyEndSignal(sigs, amqpCloseConnection)
		})
	})
})
