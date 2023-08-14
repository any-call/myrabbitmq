package myrabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type simple struct {
	*base
}

func NewSimple(connectStr string) (Simple, error) {
	b, err := newBase(connectStr)
	if err != nil {
		return nil, err
	}

	return &simple{b}, nil
}

func (b *simple) Send(qName string, timeout time.Duration, pubInfo amqp.Publishing) error {
	var err error

	err = b.EnsureConnect()
	if err != nil {
		return err
	}

	if _, err = b.setQueue(qName, false, true, false, false); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err = b.ch.PublishWithContext(ctx,
		"",
		qName, //将消息发到该队列
		false,
		false,
		pubInfo,
	); err != nil {
		return err
	}

	return nil
}

func (b *simple) Receive(qName string, f ReceivedMsg) error {
	var err error

	if _, err = b.setQueue(qName, false, true, false, false); err != nil {
		return err
	}

	msg, err2 := b.ch.Consume(qName, "", true, false, false, false, nil)
	if err2 != nil {
		return err2
	}

	go func(d <-chan amqp.Delivery, fn ReceivedMsg) {
		for v := range d {
			fn(v.Body)
		}
	}(msg, f)

	return nil
}
