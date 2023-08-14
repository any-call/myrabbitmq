package myrabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type worker struct {
	*base
}

func NewWorker(connectStr string) (Worker, error) {
	b, err := newBase(connectStr)
	if err != nil {
		return nil, err
	}

	return &worker{b}, nil
}

func (b *worker) Create(qName string, timeout time.Duration, pubInfo amqp.Publishing) error {
	var err error

	err = b.EnsureConnect()
	if err != nil {
		return err
	}

	if _, err = b.setQueue(qName, true, true, false, false); err != nil {
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

func (b *worker) Consumer(qName string, f ReceivedMsg) error {
	if _, err := b.setQueue(qName, true, true, false, false); err != nil {
		return err
	}

	if err := b.setQos(1, 0, false); err != nil {
		return err
	}

	msg, err := b.ch.Consume(qName, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	go func(d <-chan amqp.Delivery, fn ReceivedMsg) {
		for v := range d {
			fn(v.Body)
			v.Ack(false)
		}
	}(msg, f)

	return nil
}
