package rabbitmq_communicator

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/streadway/amqp"
	"time"
)

type Connection struct {
	connection                      *amqp.Connection
	serverIp                        string
	port                            int
	userName                        string
	password                        string
	reconnectInterval               time.Duration
	IsConnected                     bool
	ConnectionStatus                []chan bool
	disconnectChannel               chan bool
	connectionErrorChan             chan *amqp.Error
	registeredDisconnectionChannels []chan bool
}

/*
	Create a new connection with the given parameters:
	* IP - the ip of the RabbitMQ's broker.
	* port - the listening port of the RabbitMQ's broker.
	* user - username's of RabbitMQ's user.
	* pass - password's of RabbitMQ's user.
	* reconnectInterval - If the RabbitMQ's broker disconnect - try to reconnect every X ms according to this parameter.

	You will get a pointer to the connection.
	You will work with his object and relay on it's durability.
	If the connect to the RabbitMQ's broker disconnect - it will try to reconnect according
	to the given interval.
*/
func NewConnection(ip string, port int, user string, pass string, reconnectInterval time.Duration) *Connection {
	var connection = Connection{
		serverIp:            ip,
		port:                port,
		userName:            user,
		password:            pass,
		reconnectInterval:   reconnectInterval,
		IsConnected:         false,
		disconnectChannel:   make(chan bool),
		connectionErrorChan: make(chan *amqp.Error),
	}

	// Register myself for manual disconnection reports
	// So I'll know when to stop the watchdog when it meant to be.
	connection.NotifyManualDisconnection(connection.disconnectChannel)

	go connection.reliableConnect()

	// Start connection watchdog
	connection.ConnectionWatchdog()

	return &connection
}

/*
	Try connect to RabbitMQ according to the parameters in the object.
	Return if it succeed or not.
	This method will try to connect only once!!
	If something failed, who called it should try to do it again.

	When the connection done successfully - The method will update the connection status channel -
	so everyone will be able to handle the new connection.
*/
func (connection *Connection) connect() bool {
	if connection.IsConnected {
		return true
	}

	url := fmt.Sprintf("amqp://%s:%s@%s:%v/", connection.userName, connection.password, connection.serverIp, connection.port)

	glog.Info("Trying to log to RabbitMQ in %s", url)

	conn, err := amqp.Dial(url)

	if err != nil {
		glog.Warning("Failed to connect to RabbitMQ in url: %s. Error: %v", url, err)
		return false
	}

	glog.Info("Logged to RabbitMQ")

	connection.connection = conn

	// Update the clients
	connection.IsConnected = true
	connection.UpdateConnectionUpdate(true)

	return true
}

/*
	Reliable connect means the method won't end till the connection was established.
	After each failed connection try, the method will wait for the given duration (from the object)
	and only then will try again.

	Notice! If there is a deadlock situation - meaning it will never success to connect,
	the method will run forever. It is on your responsibility.
*/
func (connection *Connection) reliableConnect() {
	for !connection.IsConnected && !connection.connect() {
		// Connection failed! wait and try again
		time.Sleep(connection.reconnectInterval)
	}
}

/*
	Watchdog for the connection.
	Waiting for reports on disconnected connection and try to reconnect
	by the method reliableConnect on a different goroutine.
	It will stop only when the client gave a signal by "disconnectChannel" channel.

	We rely only on the SteadyWay's amqp for disconnection signals.
	If their is a failure in a different object, we will wait for this signal
	and won't act our-self.
*/
func (connection *Connection) ConnectionWatchdog() {
	connection.connection.NotifyClose(connection.connectionErrorChan)

	for {
		select {
		case err := <-connection.connectionErrorChan:
			glog.Error("Disconnected from RabbitMQ. Trying to reconnect. Error: %v", err)
			connection.UpdateConnectionUpdate(false)
			connection.reliableConnect()
		case <-connection.disconnectChannel:
			connection.Disconnect()
			glog.Info("The connection mark as disconnected. Stop trying to reconnect it")
			return
		}
	}
}

/*
	Closing the RabbitMQ's connection.
	Update all registered consumers for manual disconnection report - so they will know
	when to stop watchdog the connection.
	This method will be called ONLY when the client want to disconnect.
*/
func (connection *Connection) Disconnect() {
	if connection.IsConnected {
		return
	}

	glog.Info("Disconnecting from RabbitMQ")

	_ = connection.connection.Close()

	for _, channel := range connection.registeredDisconnectionChannels {
		channel <- true
	}
}

/*
	Register a channel to manual disconnection reports.
	When the user will want to disconnect from RabbitMQ,
	he will close only 	the connection struct and it will report the others
	to stop the watchdog.
*/
func (connection *Connection) NotifyManualDisconnection(notifyChannel chan bool) {
	connection.registeredDisconnectionChannels = append(connection.registeredDisconnectionChannels, notifyChannel)
}

/*
	Register for connection change event
*/
func (connection *Connection) NotifyConnectionChange(reportChan chan bool) {
	connection.ConnectionStatus = append(connection.ConnectionStatus, reportChan)
}

/*
	Update all the registered clients on connection status change
*/
func (connection *Connection) UpdateConnectionUpdate(status bool) {
	for _, ch := range connection.ConnectionStatus {
		ch <- status
	}
}
