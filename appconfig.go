package main

type KafkaConfig struct {
	Address              string   `xml:"address"`
	TopicGroups          []string `xml:"topicGroups>topicGroup"`
	OffsetInitial        int64    `xml:"offsetInitial"`
	OffsetCommitInterval int      `xml:"offsetCommitInterval"`
	ClientCount          int      `xml:"clientCount"`
	ApiVersion           string   `xml:"apiVersion"`
}

type AutoRestartConfig struct {
	Enable         bool  `xml:"enable"`
	ByDurationHour int64 `xml:"byDurationHour"`
	ByMsgCount     int64 `xml:"byMsgCount"`
}

type DiscardKafkaMsgConfig struct {
	Enable      bool     `xml:"enable"`
	TopicGroups []string `xml:"topicGroups>topicGroup"`
}

type AppConfig struct {
	HttpPort                  string                `xml:"httoPort"`
	Kafka                     KafkaConfig           `xml:"kafka"`
	DiscardKafkaMsg           DiscardKafkaMsgConfig `xml:"discardKafkaMsg"`
	MsgItemChannelMaxSize     uint32                `xml:"msgItemChannelMaxSize"`
	ConsumeMsgTimeoutBySecond int                   `xml:"consumeMsgTimeoutBySecond"`
	AutoRestart               AutoRestartConfig     `xml:"autoRestart"`
}

var AppConfigSingleton AppConfig
