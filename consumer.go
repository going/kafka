/**
 * Author:        Tony.Shao
 * Email:         xiocode@gmail.com
 * Github:        github.com/xiocode
 * File:          consumer.go
 * Description:   consumer
**/

package kafka

import (
	"time"

	log "github.com/golang/glog"

	kafkalib "github.com/stealthly/go_kafka_client"
)

type Consumer struct {
	consumer *kafkalib.Consumer
	config   *kafkalib.ConsumerConfig
	topics   []string
}

// 172.31.22.222:2181,172.31.21.206:2181,172.31.17.130:2181
func NewConsumer(group string, topics, zk []string, strategy kafkalib.WorkerStrategy) (*Consumer, error) {
	consumer := &Consumer{
		topics: topics,
	}

	coordinatorConfig := kafkalib.NewZookeeperConfig()
	coordinatorConfig.ZookeeperConnect = zk
	coordinatorConfig.Root = "/kafka"
	coordinator := kafkalib.NewZookeeperCoordinator(coordinatorConfig)

	consumerConfig := kafkalib.DefaultConsumerConfig()
	consumerConfig.Groupid = group
	consumerConfig.Coordinator = coordinator
	consumerConfig.Strategy = strategy
	consumerConfig.AutoOffsetReset = kafkalib.SmallestOffset
	consumerConfig.OffsetCommitInterval = 1 * time.Minute
	consumerConfig.OffsetsCommitMaxRetries = 5
	consumerConfig.MaxWorkerRetries = 5
	consumerConfig.WorkerFailureCallback = func(wm *kafkalib.WorkerManager) kafkalib.FailedDecision {
		kafkalib.Error(consumer, "Failed to write . Shutting down...")
		return kafkalib.DoNotCommitOffsetAndStop
	}
	consumerConfig.WorkerFailedAttemptCallback = func(task *kafkalib.Task, result kafkalib.WorkerResult) kafkalib.FailedDecision {
		kafkalib.Errorf(consumer, "Failed to write %s to the database after %d retries", task.Id().String(), task.Retries)
		return kafkalib.DoNotCommitOffsetAndContinue
	}
	consumer.config = consumerConfig
	consumer.consumer = kafkalib.NewConsumer(consumerConfig)
	return consumer, nil
}

func (c *Consumer) Start() {
	partitions, err := c.config.Coordinator.GetPartitionsForTopics(c.topics)
	if err != nil {
		log.Fatal(err)
	}
	c.consumer.StartStaticPartitions(partitions)
}

func (c *Consumer) Stop() <-chan bool {
	return c.consumer.Close()
}
