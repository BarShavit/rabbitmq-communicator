# rabbitmq-communicator
RabbitMQ interface for Golang applications.

## Goals
The project is a cover for streadway/amqp package.
It will give you self-recover communication via RabbitMQ.
It handles disconnections, channel errors, publishing errors and consuming errors.

After each error, all will be established again without the client will have to do anything.

## Examples

First, you will have to create 2 base structs - Connection and Channel.

You create a new connection by: `connection := rabbitmq_communicator.NewConnection("localhost", 5672, "guest", "guest", 5*time.Second)`.

The connection parameters are RabbitMQ's broker's IP and port, and RabbitMQ's user login. The last parameter will set the duration between each 
reconnect try.

You create a new channel based on a connection by: `channel := rabbitmq_communicator.NewChannel(connection, 5*time.Second)`.

The channel is created according to a connection with the same duration parameter.
You can use this before the connection established!

### Publishing

You should create one "Sender" for each publisher - exchange.

Example for "Sender" creation: `sender := rabbitmq_communicator.NewTopicExchangeSender("testExchange", false, channel, 5*time.Second)`.

It receives the exchange name, "IsDurable" flag, a channel and duration between failures.

Now, you can use it to publish message, by `sender.Send([]byte(text), "textRouting", false)`.

The parameters are the message in byte array, exchange name and persistent message flag.

Notice! A message can't be marked as persistent if the sender isn't marked as durable.

### Receiver

The receivers based on routing key.

For every routing key you want to register, you will need to create a "receiver" struct.

"Receiver" structs will be created by `	receiver := rabbitmq_communicator.NewReceiver(channel, "testExchange", "textRouting", false, 5*time.Second)`.

The parameters are a channel, exchange name, routing key, "IsDurable" flag and duration for failure situations.

Notice! You can receive a persistent messages only if the receiver marked as durable.

Now, you can get the incoming messages from RabbitMQ by using the "MessagesChannel" channel of the receiver.