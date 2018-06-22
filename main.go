package main

import (
	"encoding/xml"
	"os"
	"strings"

	"kedacom/haiou/common"

	"github.com/Shopify/sarama"
	log "github.com/lotusdeng/log4go"
)

var GlobalMsgChannel chan *sarama.ConsumerMessage

func main() {
	log.LoadConfiguration("config/kafkaproxy.xml", "xml")
	log.SetGlobalLevel(log.INFO)
	defer log.Close()

	log.Info("main start, version:", AppVersion)

	log.Info("main start open config file")
	xmlFile, err := os.Open("config/kafkaproxy.xml")
	if err != nil {
		log.Error("main load config file fail, error:", err.Error())
		return
	}
	log.Info("main open config success")
	defer xmlFile.Close()

	if err := xml.NewDecoder(xmlFile).Decode(&AppConfigSingleton); err != nil {
		log.Error("main decode config file fail, error:", err.Error())
		return
	}
	log.Info("main decode config from file success")
	log.Info("main config.kafka address:", AppConfigSingleton.Kafka.Address)
	log.Info("main config.ConsumeMsgTimeoutBySecond:", AppConfigSingleton.ConsumeMsgTimeoutBySecond)
	for i := range AppConfigSingleton.Kafka.TopicGroups {
		log.Info("main support topic group:", AppConfigSingleton.Kafka.TopicGroups[i])
		AppDataSingleton.SupportTopicGroupNameMap[AppConfigSingleton.Kafka.TopicGroups[i]] = 1
	}
	log.Info("main config.kafka apiversion:", AppConfigSingleton.Kafka.ApiVersion)

	if AppConfigSingleton.DiscardKafkaMsg.Enable {
		for _, topicGroupStr := range AppConfigSingleton.DiscardKafkaMsg.TopicGroups {
			discardKafkaMsg := new(int64)
			*discardKafkaMsg = 1
			AppDataSingleton.DiscardKafkaMsgMap[topicGroupStr] = discardKafkaMsg
		}
	}

	//GetKafkaTopicPartitionOffsets(AppConfigSingleton.Kafka.Address, AppConfigSingleton.Kafka.Topic, AppConfigSingleton.Kafka.Group)
	//return
	common.InitAppQuit()
	defer common.UinitAppQuit()

	GlobalMsgChannel = make(chan *sarama.ConsumerMessage, AppConfigSingleton.MsgItemChannelMaxSize)
	log.Info("main http url:http://127.0.0.1:", AppConfigSingleton.HttpPort)

	common.ExitWaitGroup.Add(1)
	go HttpServerLoop(AppConfigSingleton.HttpPort, common.GlobalQuitChannel)

	for _, topicGroupStr := range AppConfigSingleton.Kafka.TopicGroups {
		tokens := strings.Split(topicGroupStr, ":")
		if len(tokens) == 2 {
			topic := tokens[0]
			group := tokens[1]
			log.Info("create topic group:", topicGroupStr)
			topicGroup := &TopicGroup{Topic: topic, Group: group, MsgChannel: make(chan *sarama.ConsumerMessage, AppConfigSingleton.MsgItemChannelMaxSize),
				PartitionStatusMap:    make(map[int32]*PartitionStatus),
				BalancePartitionIdMap: make(map[int32]int)}
			AppDataSingleton.TopicGroupMap[topicGroupStr] = topicGroup
			discardKafkaMsg, exist := AppDataSingleton.DiscardKafkaMsgMap[topicGroupStr]
			if !exist {
				discardKafkaMsg = new(int64)
				AppDataSingleton.DiscardKafkaMsgMap[topicGroupStr] = discardKafkaMsg
			}
			for i := 0; i < AppConfigSingleton.Kafka.ClientCount; i += 1 {
				log.Info("http create pull msg from kafka loop, topic:", topicGroup.Topic, ", group:", topicGroup.Group)
				common.ExitWaitGroup.Add(1)
				go PullMsgFromKafkaLoop(AppConfigSingleton.Kafka.Address, topicGroup, discardKafkaMsg, common.GlobalQuitChannel)
			}
		}
	}
	if AppConfigSingleton.AutoRestart.Enable {
		log.Info("main start auto restart")
		common.ExitWaitGroup.Add(1)
		go AutoRestartLoop(common.GlobalQuitChannel)
	}
	common.ExitWaitGroup.Wait()
	log.Info("main exit")
}
