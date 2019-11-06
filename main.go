package main

import (
	"./master"
	"./server"
	"./slave"
	"time"
)

const (
	HEARTBEAT_PERIOD = 1000 * time.Millisecond
)

func main()  {
	sdfs_master := master.InitMaster()
	slave := slave.InitSlave(sdfs_master)

	slave.StartUDP()
	slave.Join()
	time.Sleep(HEARTBEAT_PERIOD)
	go slave.GetMsg()
	go slave.CheckInput()

	tcp := server.InitTCPServer(sdfs_master, slave)
	go tcp.StartTCP()

	for {
		if slave.Alive == false {
			break
		}
		time.Sleep(HEARTBEAT_PERIOD)
		go slave.HeartBeat()
	}
	slave.Leave()
}

