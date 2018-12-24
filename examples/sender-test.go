package main

import (
	"bufio"
	"flag"
	"os"
	"rabbitmq-communicator"
	"strings"
	"time"
)

func main() {
	flag.Parse()
	flag.Lookup("logtostderr").Value.Set("true")
	flag.Lookup("log_dir").Value.Set("./log")
	flag.Lookup("v").Value.Set("10")

	connection := rabbitmq_communicator.NewConnection("localhost", 5672, "guest", "guest", 5*time.Second)
	channel := rabbitmq_communicator.NewChannel(connection, 5*time.Second)
	sender := rabbitmq_communicator.NewTopicExchangeSender("testExchange", false, channel, 5*time.Second)

	reader := bufio.NewReader(os.Stdin)

	for {
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		sender.Send([]byte(text), "textRouting", false)
	}
}
