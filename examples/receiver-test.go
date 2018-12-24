package main

import (
	"flag"
	"fmt"
	"rabbitmq-communicator"
	"time"
)

func main() {
	flag.Parse()
	flag.Lookup("logtostderr").Value.Set("true")
	flag.Lookup("log_dir").Value.Set("./log")
	flag.Lookup("v").Value.Set("10")

	connection := rabbitmq_communicator.NewConnection("localhost", 5672, "guest", "guest", 5*time.Second)
	channel := rabbitmq_communicator.NewChannel(connection, 5*time.Second)
	receiver := rabbitmq_communicator.NewReceiver(channel, "testExchange", "textRouting", false, 5*time.Second)

	for msg := range receiver.MessagesChannel {
		fmt.Println(string(msg.Body))
	}
}
