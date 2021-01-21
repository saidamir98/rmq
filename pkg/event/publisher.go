package event

import (
	"fmt"

	"github.com/streadway/amqp"
)

// Publisher ...
type Publisher struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchangeName string
}

// NewPublisher ...
func NewPublisher(conn *amqp.Connection, exchangeName string) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = declareExchange(ch, exchangeName)
	if err != nil {
		fmt.Errorf("Exchange Declare: %s", err)
		return nil, err
	}

	return &Publisher{
		conn:         conn,
		channel:      ch,
		exchangeName: exchangeName,
	}, nil
}

// Push ...
func (p *Publisher) Push(routingKey string, msg amqp.Publishing) error {
	err := p.channel.Publish(
		p.exchangeName,
		routingKey,
		false,
		false,
		msg,
	)
	return err
}

// Close ...
func (p *Publisher) Close() error {
	return p.channel.Close()
}
