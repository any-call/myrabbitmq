package myrabbitmq

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type pSubscribe struct {
	*base
	switchName string
	subFun     ReceivedMsg
}

func NewPSubscribe(connectStr, sName string) (PSubscribe, error) {
	b, err := newBase(connectStr)
	if err != nil {
		return nil, err
	}

	if sName != "" {
		//自定义交换机
		if err = b.ch.ExchangeDeclare(sName,
			"fanout",
			true,
			true,
			false,
			false,
			nil); err != nil {
			return nil, err
		}
	} else {
		sName = "amq.fanout" //default exchanged
	}

	ret := &pSubscribe{base: b, switchName: sName}
	ret.base.onclose = ret.reSubscribe
	return ret, nil
}

func (self *pSubscribe) Publish(timeout time.Duration, pubInfo amqp.Publishing) error {
	err := self.EnsureConnect()
	if err != nil {
		return err
	}

	if self.ch.IsClosed() {
		if self.ch, err = self.conn.Channel(); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err = self.ch.PublishWithContext(ctx,
		self.switchName,
		"",
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
func (self *pSubscribe) Subscribe(f ReceivedMsg) error {
	var qName string
	var err error
	if qName, err = self.setQueue("", false, true, true, false); err != nil {
		return err
	}

	if err := self.setBind(self.switchName, qName, ""); err != nil {
		return err
	}
	msg, err := self.ch.Consume(qName, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	self.subFun = f
	for v := range msg {
		f(v.Body)
	}
	return nil
}

func (self *pSubscribe) reSubscribe() {
	fmt.Println(" conn is close")
	if self.subFun != nil {
		if err := self.initChannel(); err != nil {
			fmt.Println("init channel err:", err)
			return
		}

		if err := self.Subscribe(self.subFun); err != nil {
			fmt.Println("subscribe fail", err)
		}
	}
}
