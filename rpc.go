package myrabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"reflect"
	"sync"
	"time"
)

type rpcHandler struct {
	Key  string `json:"key"`
	Args []any  `json:"args"`
}

type rpcServer struct {
	sync.Mutex
	*base
	qName   string
	handler map[string]reflect.Value
}

func NewRpcServer(connectStr, qName string) (RpcServer, error) {
	if qName == "" {
		return nil, fmt.Errorf("empty queue name ")
	}

	b, err := newBase(connectStr)
	if err != nil {
		return nil, err
	}

	if _, err = b.setQueue(qName, false, true, false, false); err != nil {
		return nil, err
	}

	if err = b.setQos(1, 0, false); err != nil {
		return nil, err
	}

	msg, err := b.ch.Consume(qName, "", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	ret := &rpcServer{base: b, qName: qName, handler: make(map[string]reflect.Value, 0)}
	ret.base.onclose = ret.reSubscribe
	go ret.start(msg)

	return ret, nil
}

func (self *rpcServer) Register(fnKey string, fn any) error {
	self.Lock()
	defer self.Unlock()

	if fn == nil {
		return fmt.Errorf("callback is nil")
	}

	if reflect.TypeOf(fn).Kind() != reflect.Func {
		return fmt.Errorf("callback isn't a function ")
	}

	self.handler[fnKey] = reflect.ValueOf(fn)
	return nil
}

// Subscribe 将一个路由和队列绑定到默认的交换机上,并开始监听一个queue
func (self *rpcServer) start(msgs <-chan amqp.Delivery) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for d := range msgs {
		//从d.body 取值
		var rpcInfo *rpcHandler
		if err := json.Unmarshal(d.Body, &rpcInfo); err == nil {
			if fn, ok := self.handler[rpcInfo.Key]; ok {
				fmt.Println(fn)
			}
		}

		//if err = self.ch.PublishWithContext(ctx,
		//	"",        // exchange
		//	d.ReplyTo, // routing key
		//	false,     // mandatory
		//	false,     // immediate
		//	amqp.Publishing{
		//		ContentType:   "text/plain",
		//		CorrelationId: d.CorrelationId,
		//		Body:          []byte(err.Error()),
		//	}); err != nil {
		//	fmt.Println(" publish err:", err)
		//}
		//d.Ack(false)
	}

	return nil
}

func (self *rpcServer) reSubscribe() {
	fmt.Println(" conn is close")
	//if self.subKey != "" && self.subFun != nil {
	//	if err := self.initChannel(); err != nil {
	//		fmt.Println("init channel err:", err)
	//		return
	//	}
	//
	//	if err := self.Subscribe(self.subKey, self.subFun); err != nil {
	//		fmt.Println("subscribe fail", err)
	//	}
	//}
}
