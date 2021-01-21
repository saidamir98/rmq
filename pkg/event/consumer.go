package event

import (
	"context"
	"fmt"

	"github.com/streadway/amqp"
)

// Consumer ...
type Consumer struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchangeName string
	routingKey   string
	queueName    string
	middlewares  []func(amqp.Delivery)
	messages     <-chan amqp.Delivery
}

// NewConsumer ...
func NewConsumer(conn *amqp.Connection, exchangeName, routingKey, queueName string) (*Consumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = declareExchange(ch, exchangeName)
	if err != nil {
		fmt.Errorf("Exchange Declare: %s", err)
		return nil, err
	}

	q, err := declareQueue(ch, queueName)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(
		q.Name,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		conn:         conn,
		channel:      ch,
		exchangeName: exchangeName,
		routingKey:   routingKey,
		queueName:    q.Name,
		middlewares:  make([]func(amqp.Delivery), 0),
	}, nil
}

// Run ...
func (c *Consumer) Run(ctx context.Context, consumerName string, handler func(amqp.Delivery)) error {
	var err error
	c.messages, err = c.channel.Consume(
		c.queueName,
		consumerName,
		false,
		false,
		false,
		true,
		nil)
	if err != nil {
		return err
	}

	for {
		select {
		case msg := <-c.messages:
			for _, middleware := range c.middlewares {
				middleware(msg)
			}
			handler(msg)
		case <-ctx.Done():
			{
				c.channel.Cancel("", true)
				return nil
			}
		}
	}
}

// AppendMiddleware ...
func (c *Consumer) AppendMiddleware(f func(amqp.Delivery)) {
	c.middlewares = append(c.middlewares, f)
}
