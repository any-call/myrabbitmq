package myrabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type routing struct {
	*base
	switchName string
}

func NewRouting(connectStr, sName string) (Routing, error) {
	b, err := newBase(connectStr)
	if err != nil {
		return nil, err
	}

	//自定义交换机
	if err = b.ch.ExchangeDeclare(sName,
		"direct",
		true,
		true,
		false,
		false,
		nil); err != nil {
		return nil, err
	}

	return &routing{b, sName}, nil
}

func (self *routing) Publish(rKey string, timeout time.Duration, pubInfo amqp.Publishing) error {
	err := self.EnsureConnect()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err = self.ch.PublishWithContext(ctx,
		self.switchName,
		rKey,
		false,
		false,
		pubInfo,
	)
	if err != nil {
		return err
	}

	return nil
}

// Subscribe 将一个路由和队列绑定到默认的交换机上,并开始监听一个queue
func (self *routing) Subscribe(rKey string, f ReceivedMsg) error {
	var err error
	var qName string
	if qName, err = self.setQueue("", false, true, true, false); err != nil {
		return err
	}
	if err := self.setBind(self.switchName, qName, rKey); err != nil {
		return err
	}
	msg, err := self.ch.Consume(qName, "", true, false, false, false, nil)
	if err != nil {
		return err
	}
	for v := range msg {
		f(v.Body)
	}
	return nil
}
