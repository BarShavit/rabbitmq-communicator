package rabbitmq_communicator

import (
	"github.com/golang/glog"
	"github.com/streadway/amqp"
	"time"
)

type Receiver struct {
	channel                       *Channel
	queue                         amqp.Queue
	innerMessageChannel           <-chan amqp.Delivery
	MessagesChannel               chan amqp.Delivery
	exchangeName                  string
	routingKey                    string
	isDurable                     bool
	manualDisconnectionReportChan chan bool
	channelStatusChan             chan bool
	durationAfterCreationFailure  time.Duration
}

/*
	Creating a new receiver according to the given exchange name, routing key and IsDurable.
	The consumer will be based on RoutingKey only.
	The consume will survive disconnections and be reliable as possible.
	After each consume failure, the struct will wait according to the given duration
	till the next try
*/
func NewReceiver(channel *Channel, exchangeName string, routingKey string, isDurable bool, durationAfterFailure time.Duration) *Receiver {
	var receiver = Receiver{
		exchangeName:                  exchangeName,
		routingKey:                    routingKey,
		isDurable:                     isDurable,
		durationAfterCreationFailure:  durationAfterFailure,
		manualDisconnectionReportChan: make(chan bool),
		channelStatusChan:             make(chan bool),
		MessagesChannel:               make(chan amqp.Delivery),
		channel:                       channel,
	}

	// First consume setup
	go receiver.reliableConsumeSetup()

	go receiver.watchConsumer()

	return &receiver
}

/*
	Try to consume from RabbitMQ.
	Declare a queue and bind to it according to the struct parameters.
	After, it will start to consume the messages from the queue.

	The method will return true if it started to consume,
	else, there is some error.
*/
func (receiver *Receiver) setupConsumer() bool {
	glog.Info("Trying to setup a consumer on routing key %s", receiver.routingKey)
	q, err := receiver.channel.channel.QueueDeclare(
		"",
		receiver.isDurable,
		false,
		true,
		false,
		nil)

	if err != nil {
		glog.Warning("Failed to start a consumer on routing key %s. Error: %v", receiver.routingKey, err)
		return false
	}

	glog.Info("Created a queue")
	receiver.queue = q

	err = receiver.channel.channel.QueueBind(
		q.Name,
		receiver.routingKey,
		receiver.exchangeName,
		false,
		nil,
	)

	if err != nil {
		glog.Warning("Failed to start a consumer on routing key %s. Error: %v", receiver.routingKey, err)
		return false
	}

	msgs, err := receiver.channel.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil)

	if err != nil {
		glog.Warning("Failed to start a consumer on routing key %s. Error: %v", receiver.routingKey, err)
		return false
	}

	glog.Info("Started to consume from exchange %s on routing key %s", receiver.exchangeName, receiver.routingKey)
	go receiver.convertOutputChannels(msgs)

	return true
}

/*
	Reliable consume setup.
	It will try to consume till it succeed or the channel is off.

	Notice!!! If you reach to a situation that the setup always fail
	for some reason (and not disconnection or channel error) -
	the method will run forever
*/
func (receiver *Receiver) reliableConsumeSetup() {
	for receiver.channel.IsCreated && !receiver.setupConsumer() {
		time.Sleep(receiver.durationAfterCreationFailure)
	}
}

/*
	Watch for the consumer.
	When there is a reconnect - start the consumer from stretch.

	This method will stop watch for the consumer if we receive a report from
	the connection.
	Remember! The user has only to disconnect the connection struct, and it should
	report to all other structs to close.
*/
func (receiver *Receiver) watchConsumer() {
	receiver.channel.connection.NotifyManualDisconnection(receiver.manualDisconnectionReportChan)
	receiver.channel.NotifyChannelStatus(receiver.channelStatusChan)

	for {
		select {
		case connState := <-receiver.channelStatusChan:
			if connState {
				go receiver.reliableConsumeSetup()
			}
		case <-receiver.manualDisconnectionReportChan:
			break
		}
	}
}

/*
	We may create few consumers from AMQP,
	but we need to give the client always ONE messages channel.
	This method should be called in a different goroutine each time a new consumer
	is up, take all the messages from the consumer channel to our one output channel
*/
func (receiver *Receiver) convertOutputChannels(newChannel <-chan amqp.Delivery) {
	for msg := range newChannel {
		receiver.MessagesChannel <- msg
	}
}
