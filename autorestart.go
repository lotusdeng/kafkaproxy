package main

import (
	"kedacom/haiou/common"
	"os"
	"sync/atomic"
	"time"

	log "github.com/lotusdeng/log4go"
)

func AutoRestartLoop(quitChannel chan os.Signal) {
	defer common.ExitWaitGroup.Done()
	startTime := time.Now()
	for {
		time.Sleep(1 * time.Second)

		now := time.Now()
		var needRestart = false
		if now.Sub(startTime).Minutes() > float64(AppConfigSingleton.AutoRestart.ByDurationHour*60) {
			needRestart = true
			log.Warn("AutoRestart 时间到了，程序重启, 时间:", now.Sub(startTime).Hours(), "小时")
		} else if atomic.LoadInt64(&AppDataSingleton.GetMsgFromtKafkaCount) >= AppConfigSingleton.AutoRestart.ByMsgCount {
			needRestart = true
			log.Warn("AutoRestart 消息个数到了, 程序重启, 个数:", atomic.LoadInt64(&AppDataSingleton.GetMsgFromtKafkaCount))
		}
		if common.IsAppQuit() {
			break
		}

		if needRestart {
			log.Warn("AutoRestart PauseConsumeMsgFromKafka set 1")
			SafeStop()
			return
		}
	}
}
