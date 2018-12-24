package rabbitmq_communicator

import (
	"github.com/golang/glog"
	"github.com/streadway/amqp"
	"time"
)

type Channel struct {
	connection                      Connection
	channel                         *amqp.Channel
	durationAfterCreationFailure    time.Duration
	closingReportChan               chan *amqp.Error
	manualDisconnectionReportChan   chan bool
	registeredChansForChannelStatus []chan bool
	connectionStatusChan            chan bool
	IsCreated                       bool
}

/*
	Creating a new channel, base on the given connection.
	From here - you can rely on the channel to be always relevant.
	If there will be any disconnection reported by the connection (as should be)
	the object will recreate the channel when it's done.

	This method will start the watchdog and register for disconnection reports
	from teh connection.
*/
func NewChannel(conn Connection, durationAfterFaliure time.Duration) *Channel {
	var channel = Channel{
		conn,
		nil,
		durationAfterFaliure,
		make(chan *amqp.Error),
		make(chan bool),
		make(chan bool),
		make(chan bool),
		false,
	}

	channel.connection.NotifyManualDisconnection(channel.manualDisconnectionReportChan)

	go channel.watchChannel()

	// Trying to create the channel for the first time
	// If it fails - wait for a report from the connection.
	// If the connection is close, this method will stop on the first
	// try as explained.
	go channel.reliableCreateChannel()

	return &channel
}

/*
	Creating a new channel based on the connection from the parameter.
	The method will create it according to the inner connection.
	Meaning, when there is a reconnect for some reason,
	this class should call this method again - or we will handle only old not-relevant channel
	from the old connection.

	The method will return if the creation succeed or not.
*/
func (channel *Channel) createInnerChannel() bool {
	if !channel.connection.IsConnected {
		glog.Warning("Tried to create a channel, but the connection is closed for now.")
		return false
	}

	ch, err := channel.connection.connection.Channel()

	if err != nil {
		glog.Warning("Failed to create a channel from the given connection. Error: %v", err)
		return false
	}

	channel.channel = ch

	channel.IsCreated = true
	channel.UpdateChannelStatus(true)

	glog.Info("Created a channel.")

	return true
}

/*
	Reliable meaning the method will try to create the channel till it succeed.
	If a creation try fails, the method will wait according to the given duration (in the object).
	The method will stop try if the connection marked as close.

	Notice!!! If there is a "deadlock" situation - the channel fails to recreate always
	the method won't exit.
*/
func (channel *Channel) reliableCreateChannel() {
	glog.Info("Trying to create a channel")

	for channel.connection.IsConnected && !channel.IsCreated && !channel.createInnerChannel() {
		time.Sleep(channel.durationAfterCreationFailure)
	}
}

/*
	Watch over the channel and keep it relevant.

	Mark the channel as "Not Created" when we receive an error from RabbitMQ,
	which can be when the server disconnect or any other reason.
	Retry to create the channel if it's a special report (not disconnection one).

	Recreate the channel when we get a reconnection report.

	This method will stop watch for the channel if we receive a report from
	the connection.
	Remember! The user has only to disconnect the connection struct, and it should
	report to all other structs to close.
*/
func (channel *Channel) watchChannel() {
	channel.channel.NotifyClose(channel.closingReportChan)
	channel.connection.NotifyConnectionChange(channel.connectionStatusChan)

	for {
		select {
		case err := <-channel.closingReportChan:
			glog.Warning("RabbitMQ reported on a channel failure (because disconnection or some other reason). Error %v", err)
			channel.IsCreated = false
			channel.UpdateChannelStatus(false)
			go channel.reliableCreateChannel()
		case reconnected := <-channel.connectionStatusChan:
			if reconnected {
				go channel.reliableCreateChannel()
			}
		case <-channel.manualDisconnectionReportChan:
			break
		}
	}
}

/*
	Register a client for channel status updates.
	If something happen to the channel, it will update the clients.
*/
func (channel *Channel) NotifyChannelStatus(registerChan chan bool) {
	channel.registeredChansForChannelStatus = append(channel.registeredChansForChannelStatus, registerChan)
}

/*
	Update all the registered channels on the new channel status
*/
func (channel *Channel) UpdateChannelStatus(status bool) {
	for _, ch := range channel.registeredChansForChannelStatus {
		ch <- status
	}
}
