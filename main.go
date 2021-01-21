package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"rmq/pkg/event"
	"rmq/pkg/logger"
	"sync"
	"time"

	"encoding/json"

	"github.com/streadway/amqp"
)

var (
	// ExchangeName ...
	ExchangeName string
	// QueueName ...
	QueueName string
	// RoutingKey - Routing Key
	RoutingKey string
)

func init() {
	flag.StringVar(&QueueName, "queue", "application.queue", "Queue name")
	flag.StringVar(&ExchangeName, "exchange_name", "application", "Queue name")
	flag.StringVar(&RoutingKey, "routing_key", "*", "Queue name")
}

func main() {
	flag.Parse()

	fmt.Printf("%v\t\n%v\t\n%v\t\n", RoutingKey, ExchangeName, QueueName)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(fmt.Sprintf("connection.open: %s", err.Error()))
	}
	defer conn.Close()

	consumer, err := event.NewConsumer(conn, ExchangeName, RoutingKey, QueueName)
	if err != nil {
		panic(err)
	}

	log := logger.New("debug", "rmq")
	consumer.AppendMiddleware(func(m amqp.Delivery) {
		log.Info(string(m.Body))
	})

	publisher, err := event.NewPublisher(conn, ExchangeName)
	if err != nil {
		panic(err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(2)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		timer := time.NewTicker(1000 * time.Millisecond)
		defer timer.Stop()
	Loop:
		for {
			select {
			case <-timer.C:
				publisher.Push("random-routing-key", testMessage())
			case <-ctx.Done():
				break Loop
			}
		}
	}(&wg)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		err = consumer.Run(ctx, "tester", handler)
		if err != nil {
			panic(err)
		}
	}(&wg)

	oscall := <-c
	fmt.Printf("system call:%+v", oscall)
	cancel()
	wg.Wait()
}

func handler(msg amqp.Delivery) {
	type data struct {
		ID   string `json:"id"`
		Data string `json:"data"`
	}

	d := &data{}
	err := json.Unmarshal(msg.Body, d)
	if err != nil {
		fmt.Println(err.Error())
		msg.Nack(false, false)
		return
	}
	fmt.Printf("ContentType - \t %v \t MessageId - %v \n", msg.ContentType, msg.MessageId)
	fmt.Println(d.ID)
	msg.Ack(false)
}

var count = 0

func testMessage() amqp.Publishing {
	type testData struct {
		ID   string `json:"id"`
		Data string `json:"data"`
	}
	count++
	d := testData{
		ID:   fmt.Sprintf("%v", count),
		Data: fmt.Sprintf("MessageData %v", count),
	}
	var raw []byte
	enc, err := json.Marshal(d)
	if err == nil {
		raw = enc
	}

	return amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    d.ID,
		AppId:        "Test Application",
		Body:         raw,
	}
}
