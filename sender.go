package rabbitmq_communicator

import (
	"github.com/golang/glog"
	"github.com/streadway/amqp"
	"time"
)

type Sender interface {
	/*
		Sending a buffer to the exchange on a given routing key.

		The message will be able to mark as persistent ONLY if the client mark it in this method
		and the exchange is marked as durable.
		If the message marked as persistent, but the exchange isn't durable - the message will be
		sent in the default way - transient.

		The method returns if the message sent or not.
		If the method returns true, it doesn't mean the destination received it -
		only that the message reach to the RabbitMQ's broker.
	*/
	Send(msg []byte, routingKey string, isPersistent bool) bool
}

type TopicExchangeSender struct {
	channel                       *Channel
	durationAfterCreationFailure  time.Duration
	exchangeName                  string
	isDurable                     bool
	manualDisconnectionReportChan chan bool
	channelStatusChangeChan       chan bool
}

/*
	Creating a new topic exchange sender.
	This struct will always keep the exchange declared and will give you
	the ability to send a message on it.
*/
func NewTopicExchangeSender(exchangeName string, isDurable bool, channel *Channel, durationAfterCreationFailure time.Duration) *TopicExchangeSender {
	var sender = TopicExchangeSender{
		channel:                       channel,
		durationAfterCreationFailure:  durationAfterCreationFailure,
		exchangeName:                  exchangeName,
		isDurable:                     isDurable,
		manualDisconnectionReportChan: make(chan bool),
		channelStatusChangeChan:       make(chan bool),
	}

	return &sender
}

/*
	Declare a exchange according to the parameters.
	The supported parameters are only exchange name and IsDurable.

	IsDurable means the exchange will be declared as durable and give the
	ability to send "persistent" messages over it.
*/
func (exchange *TopicExchangeSender) createExchange() bool {
	glog.Info("Trying to create exchange %s", exchange.exchangeName)

	err := exchange.channel.channel.ExchangeDeclare(
		exchange.exchangeName,
		"topic",
		exchange.isDurable,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		glog.Warning("Failed to create exchange")
		return false
	}

	glog.Info("Created exchange %s", exchange.exchangeName)

	return true
}

/*
	Reliable create exchange meaning the system will try to declare the exchange
	till it succeed.
	After each failure, the loop will wait according to the given duration from the struct.

	Notice!!! The method will exit only if the exchange declared successfully or if the
	channel didn't create yet.
	If there is another reason to exchange declaration to ALWAYS fail - it will be blocked forever.
*/
func (exchange *TopicExchangeSender) reliableCreateExchange() {
	for exchange.channel.IsCreated && !exchange.createExchange() {
		time.Sleep(exchange.durationAfterCreationFailure)
	}
}

/*
	Watch for the exchange declaration.
	When there is a reconnect - declare it again.

	This method will stop watch for the exchange declaration if we receive a report from
	the connection.
	Remember! The user has only to disconnect the connection struct, and it should
	report to all other structs to close.
*/
func (exchange *TopicExchangeSender) watchExchangeDeclaration() {
	exchange.channel.connection.NotifyManualDisconnection(exchange.manualDisconnectionReportChan)
	exchange.channel.NotifyChannelStatus(exchange.channelStatusChangeChan)

	for {
		select {
		case connState := <-exchange.channelStatusChangeChan:
			if connState {
				go exchange.reliableCreateExchange()
			}
		case <-exchange.manualDisconnectionReportChan:
			break
		}
	}
}

/*
	Sending a buffer to the exchange on a given routing key.

	The message will be able to mark as persistent ONLY if the client mark it in this method
	and the exchange is marked as durable.
	If the message marked as persistent, but the exchange isn't durable - the message will be
	sent in the default way - transient.

	The method returns if the message sent or not.
	If the method returns true, it doesn't mean the destination received it -
	only that the message reach to the RabbitMQ's broker.
*/
func (exchange *TopicExchangeSender) Send(msg []byte, routingKey string, isPersistent bool) bool {
	var deliveryMode uint8
	if isPersistent && exchange.isDurable {
		deliveryMode = amqp.Persistent
	} else {
		deliveryMode = amqp.Transient
	}

	err := exchange.channel.channel.Publish(
		exchange.exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: deliveryMode,
			ContentType:  "text/plain",
			Body:         msg,
		},
	)

	if err != nil {
		glog.Warning("Failed to send message to routing key %s over exchange %s", routingKey, exchange.exchangeName)
		return false
	}

	return true
}
