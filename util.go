package main

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/lotusdeng/gocommon"

	"github.com/Shopify/sarama"
	log "github.com/lotusdeng/log4go"
)

func GetKafkaTopicPartitionOffsets(address string, topic string, group string) map[int32]int64 {
	var partitionOffsetMap = make(map[int32]int64)
	log.Info("address:", address, ", topic:", topic, ", group:", group)
	broker := sarama.NewBroker(address)
	err := broker.Open(nil)
	if err != nil {
		log.Error("connect fail, error:", err)
		return partitionOffsetMap
	}
	//broker.GetMetadata

	request := sarama.OffsetRequest{}
	//request.ConsumerGroup = group
	//request.ConsumerGroup = group
	response, err := broker.GetAvailableOffsets(&request)
	if err != nil {
		log.Error("fail, error:", err)
		return partitionOffsetMap
	}
	if response == nil {
		return partitionOffsetMap
	}
	log.Info(len(response.Blocks))
	log.Info(response.Blocks)
	return partitionOffsetMap
}

func SafeStop() {
	atomic.StoreInt64(&AppDataSingleton.PauseGetMsgFromKafka, 1)

	for {
		var totalMsgChannelCount int
		var freeMsgChannelCount int
		AppDataSingleton.TopicGroupMapMutex.Lock()
		for _, topicGroup := range AppDataSingleton.TopicGroupMap {
			totalMsgChannelCount += 1
			if atomic.LoadInt64(&topicGroup.MsgChannelCurrentSize) == 0 {
				freeMsgChannelCount += 1
			}
		}
		AppDataSingleton.TopicGroupMapMutex.Unlock()

		if freeMsgChannelCount == totalMsgChannelCount {
			log.Warn("AutoRestart all msg channel is empty")
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	log.Warn("AutoRestart SignalAppQuit")
	gocommon.SignalAppQuit()
	time.Sleep(1 * time.Second)
	os.Exit(1)
}
