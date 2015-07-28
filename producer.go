/**
 * Author:        Tony.Shao
 * Email:         xiocode@gmail.com
 * Github:        github.com/xiocode
 * File:          producer.go
 * Description:   kafka producer
**/
package kafka

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"

	log "github.com/golang/glog"
)

type ProducerErrorCallback func(*sarama.ProducerError)

type Producer struct {
	sarama.AsyncProducer
	client   sarama.Client
	config   *sarama.Config
	callback ProducerErrorCallback
	running  bool
	quit     chan bool
	done     chan bool
}

func NewProducer(brokers []string, name string, config *sarama.Config, cb ProducerErrorCallback) (self *Producer, err error) {
	self = &Producer{
		callback: cb,
		config:   config,
		quit:     make(chan bool),
		done:     make(chan bool),
	}
	self.client, err = sarama.NewClient(brokers, nil)

	if config == nil {
		self.AsyncProducer, err = sarama.NewAsyncProducerFromClient(self.client)
	} else {
		self.AsyncProducer, err = sarama.NewAsyncProducer(brokers, self.config)
	}

	if err != nil {
		log.Errorf("failed to create producer: %s", err)
		return nil, err
	}

	go self.Start()

	return self, nil
}

func NewFastProducer(brokers []string, cb ProducerErrorCallback) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Flush.Messages = 10000
	config.Producer.Flush.Frequency = 100 * time.Millisecond

	return NewProducer(brokers, "fast", config, cb)
}

func NewSafeProducer(brokers []string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Flush.Messages = 10000
	config.Producer.Flush.Frequency = 100 * time.Millisecond
	config.Producer.Timeout = 100 * time.Millisecond
	config.Producer.Retry.Max = 3

	return NewProducer(brokers, "safe", config, nil)
}

func NewNormalProducer(brokers []string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Flush.Messages = 10000
	config.Producer.Flush.Frequency = 100 * time.Millisecond
	config.Producer.Timeout = 100 * time.Millisecond
	config.Producer.Retry.Max = 3

	return NewProducer(brokers, "normal", config, nil)
}

func (self *Producer) Start() {
	if self.config.Producer.Return.Successes == true {
		return
	}

	self.running = true
	for {
		select {
		case err, ok := <-self.Errors():
			if ok && self.callback != nil {
				self.callback(err)
			}
		case <-self.quit:
			self.running = false
			close(self.done)
			return
		}
	}
}

func (self *Producer) Shutdown() {
	if self.running {
		log.Infof("shutting down producer run loop")
		close(self.quit)
		log.Infof("waiting for run loop to finish")
		<-self.done
	}
	log.Infof("closing producer")
	self.Close()
	log.Infof("shutdown done")
}

func (self *Producer) SendMessage(topic, key string, value []byte) (*sarama.ProducerMessage, error) {
	if value == nil || len(value) < 1 {
		return nil, errors.New("empty message")
	}

	message := &sarama.ProducerMessage{
		Topic: string(topic),
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	defer func() {
		if err := recover(); err != nil {
			log.Error(err)
		}
	}()

	self.Input() <- message

	return message, nil
}
