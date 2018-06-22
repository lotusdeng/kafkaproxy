package main

import (
	"kedacom/haiou/common"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	saramacluter "github.com/bsm/sarama-cluster"
	log "github.com/lotusdeng/log4go"
)

func PullMsgFromKafkaLoop(kafkaAddress string, topicGroup *TopicGroup, discardKafkaMsg *int64, quitChannel chan os.Signal) {
	defer common.ExitWaitGroup.Done()
	config := saramacluter.NewConfig()

	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.CommitInterval = time.Duration(AppConfigSingleton.Kafka.OffsetCommitInterval) * time.Second
	if AppConfigSingleton.Kafka.OffsetInitial == sarama.OffsetNewest {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	switch AppConfigSingleton.Kafka.ApiVersion {
	case "V0_11_0_0":
		config.Version = sarama.V0_11_0_0
	case "V0_11_0_1":
		config.Version = sarama.V0_11_0_1
	case "V1_0_0_0":
		config.Version = sarama.V1_0_0_0
	case "V1_1_0_0":
		config.Version = sarama.V1_1_0_0
	}

	brokers := strings.Split(kafkaAddress, ",")
	topics := strings.Split(topicGroup.Topic, ",")
	log.Info("consumer start connect kafka, address:", kafkaAddress, ", topic:", topicGroup.Topic,
		", group:", topicGroup.Group, " offset init:", AppConfigSingleton.Kafka.OffsetInitial, ", commit interval:", AppConfigSingleton.Kafka.OffsetCommitInterval)
	var consumer *saramacluter.Consumer
	for {
		if common.IsAppQuit() {
			log.Info("consumer IsAppQuit is true, loop break")
			return
		}
		tmpConsumer, err := saramacluter.NewConsumer(brokers, topicGroup.Group, topics, config)
		if err != nil {
			log.Info("consumer connect kafka fail, error:", err)
			if common.IsAppQuit() {
				log.Info("consumer IsAppQuit is true, loop break")
				return
			}
			time.Sleep(5 * time.Second)
			continue
		} else {
			log.Info("consumer connect kafka success")
			consumer = tmpConsumer
			break
		}
	}
	defer consumer.Close()

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Errorf("consumer receive Errors:%s", err.Error())
		}
		log.Info("consumer error loop exit")
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Infof("consumer receive Notifications: %+v", ntf)
			if ntf.Type == saramacluter.RebalanceOK {
				AppDataSingleton.TopicGroupMapMutex.Lock()
				for topicName, partitionMap := range ntf.Current {
					if topicGroup.Topic == topicName {
						for key, _ := range topicGroup.BalancePartitionIdMap {
							delete(topicGroup.BalancePartitionIdMap, key)
						}
						for _, partitionId := range partitionMap {
							topicGroup.BalancePartitionIdMap[partitionId] = 1
						}
					}

				}
				AppDataSingleton.TopicGroupMapMutex.Unlock()
			}
		}
		log.Info("consumer notification loop exit")
	}()

	// consume messages, watch signals
	for {
		if common.IsAppQuit() {
			return
		}
		if atomic.LoadInt64(&AppDataSingleton.PauseGetMsgFromKafka) == 1 {
			time.Sleep(1 * time.Second)
			continue
		}
		select {
		case msg, ok := <-consumer.Messages():
			if !ok {
				log.Error("consumer break loop")
				return
			}
			atomic.AddInt64(&AppDataSingleton.GetMsgFromtKafkaCount, 1)
			atomic.AddInt64(&topicGroup.GetMsgFromtKafkaCount, 1)
			log.Info("consumer msg, partition:", msg.Partition, ", offset:", msg.Offset, ", time:", msg.Timestamp)
			{
				topicGroup.PartitionStatusMapMutex.Lock()
				item, exist := topicGroup.PartitionStatusMap[msg.Partition]
				if exist {
					item.CurrentOffset = msg.Offset
					item.Timestamp = msg.Timestamp
				} else {
					topicGroup.PartitionStatusMap[msg.Partition] = &PartitionStatus{
						Id:            msg.Partition,
						CurrentOffset: msg.Offset,
						Timestamp:     msg.Timestamp,
					}
					atomic.AddInt64(&topicGroup.PartitionCount, 1)
				}
				topicGroup.PartitionStatusMapMutex.Unlock()
			}
			atomic.StoreInt64(&topicGroup.LastKafkaMsgPartition, int64(msg.Partition))
			atomic.StoreInt64(&topicGroup.LastKafkaMsgOffset, msg.Offset)
			if msg.Timestamp.Unix() > 0 {
				atomic.StoreInt64(&topicGroup.LastKafkaMsgTimestampBySecond, msg.Timestamp.Unix())
			}

			if atomic.LoadInt64(discardKafkaMsg) == 0 {
				topicGroup.MsgChannel <- msg
				atomic.AddInt64(&topicGroup.MsgChannelCurrentSize, 1)
			}
			consumer.MarkOffset(msg, "") // mark message as processed
		case <-quitChannel:
			log.Info("consumer receive quit signal, break loop")
			return
		}
	}
	log.Info("consumer loop exit")
}
