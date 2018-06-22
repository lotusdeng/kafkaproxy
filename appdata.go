package main

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type PartitionStatus struct {
	Id            int32
	CurrentOffset int64
	HighOffset    int64
	LowOffset     int64
	Timestamp     time.Time
}

type TopicGroup struct {
	Topic                         string
	Group                         string
	MsgChannel                    chan *sarama.ConsumerMessage
	MsgChannelMaxSize             int64
	MsgChannelCurrentSize         int64
	TotalCurrentOffset            int64
	LastViewTotalCurrentOffset    int64
	LastViewTime                  time.Time
	TotalHighOffset               int64
	PartitionCount                int64
	PartitionStatusMap            map[int32]*PartitionStatus
	PartitionStatusMapMutex       sync.Mutex
	LastKafkaMsgPartition         int64
	LastKafkaMsgOffset            int64
	LastKafkaMsgTimestampBySecond int64
	PauseGetMsgFromKafka          int64
	PauseGetMsgFromHttp           int64
	GetMsgFromtKafkaCount         int64
	GetMsgFromHttpCount           int64
	ConsumeHttpCallCount          int64
	PullMsgFromHttpCount          int64
	BalancePartitionIdMap         map[int32]int
}

type AppData struct {
	AppStartTime             time.Time
	TopicGroupMap            map[string]*TopicGroup //topic_group is key
	TopicGroupMapMutex       sync.Mutex
	GetMsgFromtKafkaCount    int64
	GetMsgFromHttpCount      int64
	PullMsgFromHttpCount     int64
	PauseGetMsgFromKafka     int64
	PauseGetMsgFromHttp      int64
	LastKafkaMsgPartition    int64
	LastKafkaMsgOffset       int64
	LastKafkaMsgTimestamp    time.Time
	SupportTopicGroupNameMap map[string]int
	DiscardKafkaMsgMap       map[string]*int64
}

var AppDataSingleton = AppData{AppStartTime: time.Now(), TopicGroupMap: make(map[string]*TopicGroup),
	SupportTopicGroupNameMap: make(map[string]int),
	DiscardKafkaMsgMap:       make(map[string]*int64)}
