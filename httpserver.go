package main

import (
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/lotusdeng/gocommon"

	"github.com/Shopify/sarama"
	log "github.com/lotusdeng/log4go"
)

type AppInfoItem struct {
	Name  string
	Value string
}

type AppStatusItem struct {
	Name  string
	Value string
}

type AppUrlItem struct {
	Name string
	Url  string
}

type AppConfItem struct {
	ConfName  string
	ConfKey   string
	ConfValue string
}

type PartitionStatusItem struct {
	Id            int32
	CurrentOffset int64
	HighOffset    int64
}
type PartitionStatusItemSlice []PartitionStatusItem

func (a PartitionStatusItemSlice) Len() int { // 重写 Len() 方法
	return len(a)
}
func (a PartitionStatusItemSlice) Swap(i, j int) { // 重写 Swap() 方法
	a[i], a[j] = a[j], a[i]
}
func (a PartitionStatusItemSlice) Less(i, j int) bool { // 重写 Less() 方法， 从大到小排序
	return a[j].Id > a[i].Id
}

type TopicGroupStatusItem struct {
	Topic                   string
	Group                   string
	PartitionCount          int64
	TotalCurrentOffset      int64
	TotalHighOffset         int64
	GetMsgFromKafkaCount    int64
	GetMsgFromHttpCount     int64
	ConsumeHttpCallCount    int64
	MsgChannelCurrentSize   int64
	SpeedPersecond          int64
	LastMsgTimestamp        string
	PartitionStatusItemList []PartitionStatusItem
}

func confHandle(w http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		fileBody, err := ioutil.ReadFile("html/conf.html")
		if err != nil {
			w.Write([]byte("read html/conf.html fail"))
			return
		}
		t, err := template.New("webpage").Parse(string(fileBody[:]))

		data := struct {
			Title           string
			AppConfItemList []AppConfItem
		}{
			Title: "KafkaProxy",
			AppConfItemList: []AppConfItem{
				{
					"日志级别[TRACE|DEBUG|INFO|ERROR]",
					"LogLevel",
					fmt.Sprint(log.GetGlobalLevel()),
				},
				{
					"暂停从kafka获取消息[1|0]",
					"PauseGetMsgFromKafka",
					strconv.FormatInt(atomic.LoadInt64(&AppDataSingleton.PauseGetMsgFromKafka), 10),
				},
				{
					"暂停从http获取消息[1|0]",
					"PauseGetMsgFromHttp",
					strconv.FormatInt(atomic.LoadInt64(&AppDataSingleton.PauseGetMsgFromHttp), 10),
				},
			},
		}
		for topicGroupStr, discardKafkaMsg := range AppDataSingleton.DiscardKafkaMsgMap {
			data.AppConfItemList = append(data.AppConfItemList, AppConfItem{
				"丢弃" + topicGroupStr + "的kafka消息",
				topicGroupStr + "_discardkafkamsg",
				fmt.Sprint(*discardKafkaMsg),
			})
		}
		err = t.Execute(w, data)
	} else {
		var (
			confKey   string = req.PostFormValue("ConfKey")
			confValue string = req.PostFormValue(confKey)
		)
		log.Info("http confKey:", confKey, ", confValue:", confValue)
		switch confKey {
		case "LogLevel":
			switch confValue {
			case "TRACE":
				log.SetGlobalLevel(log.TRACE)
			case "DEBUG":
				log.SetGlobalLevel(log.DEBUG)
			case "INFO":
				log.SetGlobalLevel(log.INFO)
			case "ERROR":
				log.SetGlobalLevel(log.ERROR)
			}
		case "PauseGetMsgFromKafka":
			switch confValue {
			case "1":
				atomic.StoreInt64(&AppDataSingleton.PauseGetMsgFromKafka, 1)
			case "0":
				atomic.StoreInt64(&AppDataSingleton.PauseGetMsgFromKafka, 0)
			}
		case "PauseGetMsgFromHttp":
			switch confValue {
			case "1":
				atomic.StoreInt64(&AppDataSingleton.PauseGetMsgFromHttp, 1)
			case "0":
				atomic.StoreInt64(&AppDataSingleton.PauseGetMsgFromHttp, 0)
			}
		}
		for topicGroupStr, discardKafkaMsg := range AppDataSingleton.DiscardKafkaMsgMap {
			if confKey == topicGroupStr+"_discardkafkamsg" {
				switch confValue {
				case "1":
					atomic.StoreInt64(discardKafkaMsg, 1)
				case "0":
					atomic.StoreInt64(discardKafkaMsg, 0)
				}
				break
			}
		}
		w.Write([]byte("修改成功"))
	}

}

var LastViewPartitionStatusItemList []PartitionStatusItem

func indexHandle(w http.ResponseWriter, req *http.Request) {
	log.Info("http /")
	fileBody, err := ioutil.ReadFile("html/kafkaproxy_index.html")
	if err != nil {
		w.Write([]byte("dpssgate read html/kafkaproxy_index.html fail"))
		return
	}
	t, err := template.New("webpage").Parse(string(fileBody[:]))

	data := struct {
		AppInfoItemList          []AppInfoItem
		AppConfItemList          []AppConfItem
		AppStatusItemList        []AppStatusItem
		AppUrlItemList           []AppUrlItem
		TopicGroupStatusItemList []TopicGroupStatusItem
	}{
		AppInfoItemList: []AppInfoItem{
			{
				"版本",
				AppVersion,
			},
			{
				"程序启动时间",
				AppDataSingleton.AppStartTime.Format("2006-01-02T15:04:05"),
			},
			{
				"当前时间",
				time.Now().Format("2006-01-02T15:04:05"),
			},
			{
				"运行时间",
				func() string {
					du := time.Now().Sub(AppDataSingleton.AppStartTime)
					return fmt.Sprintf("%d天%d小时%d分%d秒", int64(du.Hours())/24, int64(du.Hours())%24, int64(du.Minutes())%60, int64(du.Seconds())%60)
				}(),
			},
		},
		AppConfItemList: []AppConfItem{
			{
				"Kafka地址",
				"",
				AppConfigSingleton.Kafka.Address,
			},
			{
				"消息队列最大长度",
				"",
				fmt.Sprint(AppConfigSingleton.MsgItemChannelMaxSize),
			},
			{
				"Http获取消息的超时时间",
				"",
				strconv.Itoa(AppConfigSingleton.ConsumeMsgTimeoutBySecond) + "秒",
			},
		},
		AppStatusItemList: []AppStatusItem{
			{
				"收到的Kafka消息个数",
				strconv.FormatInt(atomic.LoadInt64(&AppDataSingleton.GetMsgFromtKafkaCount), 10),
			},
			{
				"提取走Kafka消息个数",
				strconv.FormatInt(atomic.LoadInt64(&AppDataSingleton.GetMsgFromHttpCount), 10),
			},
		},
		AppUrlItemList: []AppUrlItem{
			{
				"强制停止服务",
				"/stop",
			},
			{
				"优雅停止服务",
				"/safestop",
			},
			{
				"修改配置",
				"/conf",
			},
		},
		TopicGroupStatusItemList: []TopicGroupStatusItem{},
	}

	for i := range AppConfigSingleton.Kafka.TopicGroups {
		data.AppConfItemList = append(data.AppConfItemList, AppConfItem{
			fmt.Sprintf("支持Topic-Group-%d", i),
			"",
			AppConfigSingleton.Kafka.TopicGroups[i],
		})
	}

	AppDataSingleton.TopicGroupMapMutex.Lock()

	for _, topicGroup := range AppDataSingleton.TopicGroupMap {
		topicGroupStatusItem := TopicGroupStatusItem{
			Topic:                   topicGroup.Topic,
			Group:                   topicGroup.Group,
			PartitionCount:          topicGroup.PartitionCount,
			TotalCurrentOffset:      atomic.LoadInt64(&topicGroup.TotalCurrentOffset),
			TotalHighOffset:         atomic.LoadInt64(&topicGroup.TotalHighOffset),
			GetMsgFromKafkaCount:    atomic.LoadInt64(&topicGroup.GetMsgFromtKafkaCount),
			GetMsgFromHttpCount:     atomic.LoadInt64(&topicGroup.GetMsgFromHttpCount),
			ConsumeHttpCallCount:    atomic.LoadInt64(&topicGroup.ConsumeHttpCallCount),
			MsgChannelCurrentSize:   atomic.LoadInt64(&topicGroup.MsgChannelCurrentSize),
			LastMsgTimestamp:        time.Unix(atomic.LoadInt64(&topicGroup.LastKafkaMsgTimestampBySecond), 0).Format("2006-01-02T15:04:05"),
			PartitionStatusItemList: []PartitionStatusItem{},
		}
		for partitionId, partitionStatus := range topicGroup.PartitionStatusMap {
			_, exist := topicGroup.BalancePartitionIdMap[partitionId]
			if !exist {
				continue
			}
			topicGroupStatusItem.PartitionStatusItemList = append(topicGroupStatusItem.PartitionStatusItemList, PartitionStatusItem{
				Id:            partitionId,
				CurrentOffset: partitionStatus.CurrentOffset,
				HighOffset:    partitionStatus.HighOffset,
			})
			topicGroupStatusItem.TotalCurrentOffset += partitionStatus.CurrentOffset
			topicGroupStatusItem.TotalHighOffset += partitionStatus.HighOffset
		}
		if atomic.LoadInt64(&topicGroup.LastViewTotalCurrentOffset) != 0 {
			countDuration := topicGroupStatusItem.TotalCurrentOffset - atomic.LoadInt64(&topicGroup.LastViewTotalCurrentOffset)
			timeMillisecondDurtation := int64(time.Now().Sub(topicGroup.LastViewTime).Seconds() * 1000)
			if timeMillisecondDurtation > 0 {
				speed := countDuration / timeMillisecondDurtation
				topicGroupStatusItem.SpeedPersecond = speed * 1000
			}
		}
		atomic.StoreInt64(&topicGroup.LastViewTotalCurrentOffset, topicGroupStatusItem.TotalCurrentOffset)
		topicGroup.LastViewTime = time.Now()

		sort.Sort(PartitionStatusItemSlice(topicGroupStatusItem.PartitionStatusItemList))
		data.TopicGroupStatusItemList = append(data.TopicGroupStatusItemList, topicGroupStatusItem)

	}
	AppDataSingleton.TopicGroupMapMutex.Unlock()

	err = t.Execute(w, data)

}

func stopHandle(w http.ResponseWriter, req *http.Request) {
	log.Warn("http /stop")
	w.Write([]byte("stop ok"))
	gocommon.SignalAppQuit()
	os.Exit(1)
}

func safeStopHandle(w http.ResponseWriter, req *http.Request) {
	log.Warn("http /stop")
	w.Write([]byte("stop ok"))
	SafeStop()
}

func produceHandle(w http.ResponseWriter, req *http.Request) {

}

func consumeHandle(w http.ResponseWriter, req *http.Request) {
	atomic.AddInt64(&AppDataSingleton.PullMsgFromHttpCount, 1)

	req.ParseForm()
	topic := req.Form.Get("topic")
	group := req.Form.Get("group")
	log.Info("http consume topic:", topic, ", group:", group)
	if topic == "" || group == "" {
		w.Header().Add("Result", "")
		w.Write([]byte("miss topic or group"))
		return
	}
	topicGroupStr := topic + ":" + group
	AppDataSingleton.TopicGroupMapMutex.Lock()
	topicGroup, exist := AppDataSingleton.TopicGroupMap[topicGroupStr]
	if !exist {
		topicGroup = &TopicGroup{Topic: topic, Group: group, MsgChannel: make(chan *sarama.ConsumerMessage, AppConfigSingleton.MsgItemChannelMaxSize),
			PartitionStatusMap:    make(map[int32]*PartitionStatus),
			BalancePartitionIdMap: make(map[int32]int)}
		AppDataSingleton.TopicGroupMap[topicGroupStr] = topicGroup
		discardKafkaMsg, exist := AppDataSingleton.DiscardKafkaMsgMap[topicGroupStr]
		if !exist {
			discardKafkaMsg = new(int64)
			*discardKafkaMsg = 0
			AppDataSingleton.DiscardKafkaMsgMap[topicGroupStr] = discardKafkaMsg
		}
		for i := 0; i < AppConfigSingleton.Kafka.ClientCount; i += 1 {
			log.Info("http create pull msg from kafka loop, topic:", topicGroup.Topic, ", group:", topicGroup.Group)
			gocommon.ExitWaitGroup.Add(1)
			go PullMsgFromKafkaLoop(AppConfigSingleton.Kafka.Address, topicGroup, discardKafkaMsg, gocommon.GlobalQuitChannel)
		}
	}
	AppDataSingleton.TopicGroupMapMutex.Unlock()

	if atomic.LoadInt64(&topicGroup.PauseGetMsgFromHttp) == 1 {
		time.Sleep(time.Second * time.Duration(AppConfigSingleton.ConsumeMsgTimeoutBySecond))
		w.Write([]byte("no msg"))
	}

	atomic.AddInt64(&topicGroup.ConsumeHttpCallCount, 1)
	log.Info("http wait a msg from msg channel")
	select {
	case msg := <-topicGroup.MsgChannel:
		atomic.AddInt64(&topicGroup.MsgChannelCurrentSize, -1)
		w.Write(msg.Value)
		atomic.AddInt64(&topicGroup.GetMsgFromHttpCount, 1)
		atomic.AddInt64(&AppDataSingleton.GetMsgFromHttpCount, 1)
	case <-time.After(time.Second * time.Duration(AppConfigSingleton.ConsumeMsgTimeoutBySecond)):
		w.Write([]byte("no msg"))
		log.Info("http wait msg timeout")
	}
}

func HttpServerLoop(httpPort string, quitChannel chan os.Signal) {
	defer gocommon.ExitWaitGroup.Done()
	http.HandleFunc("/", indexHandle)
	http.HandleFunc("/stop", stopHandle)
	http.HandleFunc("/safestop", safeStopHandle)
	http.HandleFunc("/consume", consumeHandle)
	http.HandleFunc("/produce", produceHandle)
	http.HandleFunc("/conf", confHandle)
	//var dir = path.Dir(os.Args[0])
	//log.Info(dir)
	//http.Handle("/file/", http.FileServer(http.Dir(dir)))

	httpServer := &http.Server{
		Addr:    ":" + AppConfigSingleton.HttpPort,
		Handler: http.DefaultServeMux,
	}

	gocommon.ExitWaitGroup.Add(1)
	go func() {
		defer gocommon.ExitWaitGroup.Done()
		<-quitChannel
		log.Info("http receive quit signal, http server close")
		httpServer.Close()
	}()

	log.Info("http listen and serve, url:http://127.0.0.1:", AppConfigSingleton.HttpPort)
	httpServer.ListenAndServe()
	log.Info("http server serve end")
}
