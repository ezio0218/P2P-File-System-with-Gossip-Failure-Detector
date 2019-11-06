package slave

import (
	"../logger"
	"../master"
	"../sdfs_slave"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	INTRODUCER_ADDR  = "172.22.157.12"
	MIN_NODE_NUM     = 4
	PERIOD           = 5000000000
	COOLDOWN         = 5000000000
	SDFS_FILE_PREFIX = "sdfs/"
	HEARTBEAT_PERIOD = 1000 * time.Millisecond
	MY_DIR           = ""
)

type Replica_Recover_request struct {
	Replica_address string
	Local_filename  string
	Sdfs_filename   string
	Version         int
}

type PutRequest struct {
	SDFSFilename string
	Adress       string
}

type Confirmation struct {
	Command string
	Time    time.Time
}

type Put_response_info struct {
	Put_or_not bool
	Response   master.Put_response
}

type Vote_info struct {
	Vote     bool
	Vote_num int
	Voters   map[string]int
}

type Slave struct {
	Address        string
	Conn           *net.UDPConn
	Alive          bool
	MemberList     []*master.Member
	Neighbors      []string
	RecentFailList []*master.Member
	VoteStatus     *Vote_info
	SDFSInfo       *sdfs_slave.SDFSSLAVE
	SDFSMaster     *master.SDFSMaster
	master         string
	SDFSProgress   map[string][]*GetResponse
	Lock           sync.Mutex
}

type ClientRequest struct {
	Greppattern string
	Flag        string
	Server_id   string
}

type GetRequest struct {
	SDFSFilename string
	Version      int
}

type GetResponse struct {
	Version int
	Address string
}

type Got_File_Info struct {
	Address_list []string
	Version      int
}

func InitSlave(mas *master.SDFSMaster) *Slave {
	new_slave := new(Slave)
	new_slave.Address = get_self_address()
	new_slave.Alive = false
	new_slave.master = INTRODUCER_ADDR
	map1 := make(map[string][]*GetResponse)
	map2 := make(map[string]int)
	var list1 []*master.Member
	var list2 []string
	var list3 []*master.Member
	var lock sync.Mutex
	new_slave.MemberList = list1
	new_slave.Neighbors = list2
	new_slave.RecentFailList = list3
	new_slave.VoteStatus = new(Vote_info)
	new_slave.VoteStatus.Voters = map2
	new_slave.VoteStatus.Vote = false
	new_slave.VoteStatus.Vote_num = 0
	new_slave.SDFSInfo = sdfs_slave.Init_SDFSSlave()
	new_slave.SDFSMaster = mas
	new_slave.SDFSProgress = map1
	new_slave.Lock = lock
	return new_slave
}

type Pair struct {
	Key   string
	Value int
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value > p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func sortByValue(list []map[string]int) []Pair {
	pl := make(PairList, len(list))
	i := 0
	for _, pair := range list {
		for k, v := range pair {
			pl[i] = Pair{k, v}
			i++
		}
	}
	sort.Sort(sort.Reverse(pl))

	return pl
}

func get_self_address() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err)
	}
	var ip string = "localhost"
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip = ipnet.IP.String()
			}
		}
	}
	return ip
}

func InitMembership(ipaddress string, heartBeatCount int, updateTime int64) (member *master.Member) {
	member = &master.Member{
		UpdateTime:     updateTime,
		Address:        ipaddress,
		HeartbeatCount: heartBeatCount}
	return member
}

func (self *Slave) StartUDP() {
	name, err := os.Hostname()
	if err != nil {
		log.Fatal("get Hostname error", err)
		os.Exit(1)
	}
	address, err := net.LookupHost(name)
	if err != nil {
		log.Fatal("get Host Address error", err)
		os.Exit(1)
	}
	host := net.JoinHostPort(address[0], "8000")
	log.Println("New Machine added with address: ", host)
	udpaddress, err := net.ResolveUDPAddr("udp", host)
	if err != nil {
		log.Fatal("Bind UDP Address error", err)
		return
	}
	conn, err := net.ListenUDP("udp", udpaddress)
	if err != nil {
		return
	}
	self.Conn = conn
	self.Address = address[0]
	self.Alive = true
	logger.WriteLog("UDP Server running on this local machine!")
	log.Println("UDP Server running on this local machine!")
}

func (self *Slave) MemberInList(senderIP string) bool {
	for _, member := range self.MemberList {
		if senderIP == member.Address {
			return true
		}
	}
	return false
}

func (self *Slave) GetMsg() {
	if self.Alive == true {
		for {
			buf := make([]byte, 1024)
			n, addr, err := self.Conn.ReadFromUDP(buf)

			if err != nil {
				log.Panic("%d Message received from %s with error %s\n", n, addr.String(), err)
				continue
			}
			b := buf[:n]
			receive := strings.SplitN(string(b), "<CMD>", 2)
			senderIP := addr.IP.String()
			if len(receive) > 1 {
				command := receive[1]
				logger.WriteLog("Command received from " + senderIP + "is" + receive[1])
				fmt.Println("Command received from %s: %s", senderIP, receive[1])
				if command == "JOIN" {
					logger.WriteLog("New Member ready to join with IP: " + senderIP)
					fmt.Println("New Member ready to join with IP: ", senderIP)
					var judge bool
					judge = self.MemberInList(senderIP)
					if judge == false {
						self.addNewMember(senderIP)
					}
				} else if command == "LEAVE" {
					logger.WriteLog("The Sender want to Leave the group, its IP is: " + senderIP)
					fmt.Println("The Sender want to Leave the group, its IP is: ", senderIP)
					self.removeMember(senderIP)
				} else if command == "REMOVE" {
					logger.WriteLog("The Sender want me to delete one process, its IP is: " + receive[0])
					fmt.Println("The Sender want me to delete one process, its IP is: " + receive[0])
					self.removeMember(receive[0])
				}
			} else {
				logger.WriteLog("Received Gossip from Sender: " + senderIP)
				new_receive := decode(string(b))
				self.MergeMemberList(new_receive)
			}
		}
	}
}

func (self *Slave) addNewMember(address string) (newMember *master.Member) {
	now := time.Now()
	newMember = InitMembership(address, 0, now.UnixNano())
	logger.WriteLog("Added new member: " + address)
	log.Printf("Added new member: ", address)
	self.MemberList = append(self.MemberList, newMember)
	message := encode(self.MemberList)
	for _, member := range self.MemberList {
		logger.WriteLog("Send New Member List to : " + member.Address)
		host := net.JoinHostPort(member.Address, "8000")
		addr, err := net.ResolveUDPAddr("udp", host)
		if err != nil {
			log.Panic("Error when solving Member IP when heartbeating", err)
		}
		con, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			log.Panic("Error when dialing to Gossip", err)
		}
		_, err = con.Write([]byte(message))
		if err != nil {
			log.Panic("Sending Gossiping Request Through UDP Error:", err)
		}
	}
	return
}

func (self *Slave) removeMember(address string) {
	remove_index := getIndex(address, self.MemberList)
	index := getIndex(address, self.RecentFailList)
	if index == -1 {
		self.RecentFailList = append(self.RecentFailList, self.MemberList[remove_index])
	}
	if remove_index != -1 {
		self.MemberList = append(self.MemberList[:remove_index], self.MemberList[remove_index+1:]...)
		logger.WriteLog("Removed Address from local member list : " + address)
	}
}

func (self *Slave) Join() {
	var message []string
	var send_message string
	message = append(message, self.Address)
	message = append(message, "JOIN")
	send_message = strings.Replace(strings.Trim(fmt.Sprint(message), "[]"), " ", "<CMD>", -1)
	host := net.JoinHostPort(self.master, "8000")
	addr, err := net.ResolveUDPAddr("udp", host)
	logger.WriteLog("Join the network and send request to" + host)
	if err != nil {
		log.Panic("Error when solving Introducer IP", err)
	}
	con, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Panic("Error when dialing to Join", err)
	}
	_, err = con.Write([]byte(send_message))
	if err != nil {
		log.Panic("Sending Join Request Through UDP Error:", err)
	}
}

func (self *Slave) Leave() {
	var message []string
	var send_message string
	message = append(message, self.Address)
	message = append(message, "LEAVE")
	send_message = strings.Replace(strings.Trim(fmt.Sprint(message), "[]"), " ", "<CMD>", -1)
	for _, member := range self.MemberList {
		if member.Address == self.Address {
			continue
		}
		logger.WriteLog("Send Leave Request to the network" + member.Address)
		host := net.JoinHostPort(member.Address, "8000")
		addr, err := net.ResolveUDPAddr("udp", host)
		if err != nil {
			log.Panic("Error when solving Member IP", err)
		}
		con, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			log.Panic("Error when dialing to Leave", err)
		}
		_, err = con.Write([]byte(send_message))
		if err != nil {
			log.Panic("Sending Leave Request Through UDP Error:", err)
		}
		self.Alive = false
	}
}

func (self *Slave) Remove(address string) {
	var message []string
	var send_message string
	message = append(message, address)
	message = append(message, "REMOVE")
	send_message = strings.Replace(strings.Trim(fmt.Sprint(message), "[]"), " ", "<CMD>", -1)
	for _, member := range self.MemberList {
		if member.Address == self.Address {
			continue
		}
		logger.WriteLog("Send Remove " + address + "Request to the network" + member.Address)
		host := net.JoinHostPort(member.Address, "8000")
		addr, err := net.ResolveUDPAddr("udp", host)
		if err != nil {
			log.Panic("Error when solving Member IP", err)
		}
		con, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			log.Panic("Error when dialing to Leave", err)
		}
		_, err = con.Write([]byte(send_message))
		if err != nil {
			log.Panic("Sending Leave Request Through UDP Error:", err)
		}
	}
}

func encode(s []*master.Member) (vv string) {
	var d []string
	for _, ss := range s {
		sentence := string(ss.Address) + "<#INFO#>" + strconv.Itoa(ss.HeartbeatCount) + "<#INFO#>" + strconv.FormatInt(ss.UpdateTime, 10)
		d = append(d, sentence)
	}
	send_message := strings.Replace(strings.Trim(fmt.Sprint(d), "[]"), " ", "<#ENTRY#>", -1)
	return send_message
}

func decode(vv string) (s []*master.Member) {
	var d []*master.Member
	receive := strings.Split(string(vv), "<#ENTRY#>")
	for _, ss := range receive {
		sentence := strings.SplitN(string(ss), "<#INFO#>", 3)
		count, _ := strconv.Atoi(sentence[1])
		time, _ := strconv.ParseInt(sentence[2], 10, 64)
		d = append(d, InitMembership(sentence[0], count, time))
	}
	return d
}

func IsMemberExist(m *master.Member, s []*master.Member) bool {
	for _, ss := range s {
		if m.Address == ss.Address {
			return true
		}
	}
	return false
}

func GetIndex(m *master.Member, s []*master.Member) int {
	for index, ss := range s {
		if m.Address == ss.Address {
			return index
		}
	}
	return -1
}

func getIndex(m string, s []*master.Member) int {
	for index, ss := range s {
		if m == ss.Address {
			return index
		}
	}
	return -1
}

func (self *Slave) MergeMemberList(s []*master.Member) {
	if len(s) == 0 {
		return
	}
	for local_index, localmember := range self.MemberList {
		judge := IsMemberExist(localmember, s)
		if judge == false {
			continue
		} else if judge == true {
			index := GetIndex(localmember, s)
			if localmember.HeartbeatCount < s[index].HeartbeatCount {
				self.MemberList[local_index].HeartbeatCount = s[index].HeartbeatCount
				self.MemberList[local_index].UpdateTime = time.Now().UnixNano()
			}
		}
	}
	for _, remotemember := range s {
		judge2 := IsMemberExist(remotemember, self.MemberList)
		judge3 := IsMemberExist(remotemember, self.RecentFailList)
		if judge2 == true {
			continue
		} else if judge2 == false && judge3 == false {
			now := time.Now()
			self.MemberList = append(self.MemberList, InitMembership(remotemember.Address, remotemember.HeartbeatCount, now.UnixNano()))
		}
	}
}

func (self *Slave) updateMemberList() {
	for _, member := range self.MemberList {
		if member.Address == self.Address {
			member.UpdateTime = time.Now().UnixNano()
			member.HeartbeatCount++
		}
	}
	self.detectfailure()
	self.cleanFailList()

	for _, member := range self.MemberList {
		if self.master == member.Address {
			return
		}
	}
	self.revote_master()
}

func (self *Slave) detectfailure() {
	//Postive Failure
	current := time.Now().UnixNano()
	need_to_recover := false
	for _, member := range self.MemberList {
		if member.Address == self.Address {
			continue
		}
		if member.HeartbeatCount <= 1 {
			continue
		} else if member.HeartbeatCount > 1 && member.UpdateTime < current-PERIOD {
			need_to_recover = true
			self.removeMember(member.Address)
			self.Remove(member.Address)
			logger.WriteLog("Failure Detected of: " + member.Address)
			fmt.Println("Failure Detected of: ", member.Address)
		}
	}
	self.SDFSMaster.Update_member(self.MemberList)
	if need_to_recover == true {
		go self.Fail_recover()
	}
}

func (self *Slave) cleanFailList() {
	if len(self.RecentFailList) < 1 {
		return
	}
	current := time.Now().UnixNano()
	for i := 0; i < len(self.RecentFailList); {
		if self.RecentFailList[i].UpdateTime < current-COOLDOWN {
			logger.WriteLog("RecentFailList release Address: " + self.RecentFailList[i].Address)
			self.RecentFailList = append(self.RecentFailList[:i], self.RecentFailList[i+1:]...)
		} else {
			i++
		}
	}
}

func (self *Slave) HeartBeat() {
	if self.Alive == false {
		return
	}
	// Nobody in the list yet
	if len(self.MemberList) < 4 {
		for _, member := range self.MemberList {
			member.UpdateTime = time.Now().UnixNano()
		}
		return
	}
	//Not Enough to get 3 neighbor,update time but do nothing
	if self.Alive == true && len(self.MemberList) >= 4 {
		var neighbor []int
		var self_index int
		self.updateMemberList()
		self_index = getIndex(self.Address, self.MemberList)
		member_list_len := len(self.MemberList)
		neighbor = append(neighbor, (self_index-1)%member_list_len)
		neighbor = append(neighbor, (self_index+1)%member_list_len)
		neighbor = append(neighbor, (self_index+2)%member_list_len)
		for index, value := range neighbor {
			if value < 0 {
				neighbor[index] = value + member_list_len
			}
		}
		message := encode(self.MemberList)

		for _, neighbor_index := range neighbor {
			logger.WriteLog("Send new Membership list to: " + self.MemberList[neighbor_index].Address)
			host := net.JoinHostPort(self.MemberList[neighbor_index].Address, "8000")
			addr, err := net.ResolveUDPAddr("udp", host)
			if err != nil {
				log.Panic("Error when solving Member IP when heartbeating", err)
			}
			con, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				log.Panic("Error when dialing to Gossip", err)
			}
			_, err = con.Write([]byte(message))
			if err != nil {
				log.Panic("Sending Gossiping Request Through UDP Error:", err)
			}
		}
	}
}

func (self *Slave) CheckInput() {
	var input string
	for {
		fmt.Scanln(&input)
		if input == "leave" {
			self.Alive = false
			self.Leave()
		}
		if input == "join" {
			self.Alive = true
			self.Join()
		}
		if input == "lsm" {
			for _, member := range self.MemberList {
				fmt.Println("Local Members are: ", member)
			}
		}
		if input == "IP" {
			fmt.Println("Local IP is: ", self.Address)

		}
		if input == "put" {
			var local_filename string
			var sdfs_filename string
			_, err := fmt.Scanln(&local_filename, &sdfs_filename)
			if err != nil {
				fmt.Println("error with reading user's input")
				continue
			}
			self.put(local_filename, sdfs_filename)
		}
		if input == "get" {
			var local_filename string
			var sdfs_filename string
			_, err := fmt.Scanln(&sdfs_filename, &local_filename)
			if err != nil {
				fmt.Println("error with reading user's input")
				continue
			}
			self.Get(sdfs_filename, local_filename)
		}
		if input == "delete" {
			var sdfs_filename string
			_, err := fmt.Scanln(&sdfs_filename)
			if err != nil {
				fmt.Println("error with reading user's input")
				continue
			}
			self.delete(sdfs_filename)
		}
		if input == "ls" {
			var sdfs_filename string
			_, err := fmt.Scanln(&sdfs_filename)
			if err != nil {
				fmt.Println("error with reading user's input")
				continue
			}
			self.ls(sdfs_filename)
		}
		if input == "store" {
			self.store()
		}
		if input == "check" {
			self.show_metadata()
		}
	}

}

func (self *Slave) show_metadata() {
	fmt.Print("the current meta data length is ")
	fmt.Println(len(self.SDFSMaster.File_matadata))
	for key, value := range self.SDFSMaster.File_matadata {
		fmt.Print("filename: ")
		fmt.Println(key)
		fmt.Print("node list is ")
		for _, node := range value.Node_list {
			fmt.Print(node)
		}

	}
}

//func startTCP() {
//	//
//	service := new(Service)
//	rpc.Register(service)
//	address, error := net.ResolveTCPAddr("tcp", ":9000")
//	if error != nil {
//		log.Fatal("Address resolving Error: ", error)
//	}
//	listener, error := net.ListenTCP("tcp", address)
//	fmt.Println("Server started")
//	if error != nil {
//		log.Fatal("Listening establishment Error: ", error)
//	}
//	for {
//		conn, error := listener.Accept()
//		if error != nil {
//			continue
//		}
//		go rpc.ServeConn(conn)
//	}
//}

//func getAdrress(ip string) *net.UDPAddr{
//	ip_port := net.JoinHostPort(ip, "8000")
//	udpaddress, err := net.ResolveUDPAddr("udp", ip_port)
//	if err != nil {
//		log.Fatal("Bind UDP Address error", err)
//		return nil
//	}
//	return udpaddress
//}

func (self *Slave) InitWork(sdfs_filename string) {
	self.Lock.Lock()
	var list1 []*GetResponse
	self.SDFSProgress[sdfs_filename] = list1
	self.Lock.Unlock()
}

func (self *Slave) put(local_filename string, sdfs_filename string) bool {
	host := net.JoinHostPort(self.master, "9000")
	client, err := rpc.Dial("tcp", host)
	if err != nil {
		log.Fatal("error with connecting to master", err)
	}
	defer client.Close()
	request := PutRequest{sdfs_filename, self.Address}

	put_response := new(Put_response_info)
	err = client.Call("TCPServer.Get_put_info", request, put_response)

	//put_response := get_put_info(slave, sdfs_filename)
	if put_response.Put_or_not == false {
		logger.WriteLog("Write-Write conflicts! Operation ended!")
		fmt.Println("Write-Write conflicts! Operation ended!")
		//log.Fatal("Write-Write conflicts! Operation ended!")
		return false
	}
	self.InitWork(sdfs_filename)
	version := put_response.Response.Version

	var wg sync.WaitGroup
	for _, replica_address := range put_response.Response.Ip_list {
		fmt.Println("Putting file to " + replica_address + "with version of " + strconv.Itoa(version))
		wg.Add(1)
		go self.Put_to_replica(replica_address, local_filename, sdfs_filename, version, &wg)
	}
	wg.Wait()

	for {
		time.Sleep(HEARTBEAT_PERIOD)
		if len(self.SDFSProgress[sdfs_filename]) >= self.cal_quorum_num(len(put_response.Response.Ip_list)) {
			quorum_log_info := "quorum count: " + strconv.Itoa(len(self.SDFSProgress[sdfs_filename]))
			logger.WriteLog(quorum_log_info)
			log.Println(quorum_log_info)
			put_success_info := "put succeed: " + sdfs_filename + " " + strconv.Itoa(version)
			fmt.Println(put_success_info)
			logger.WriteLog(put_success_info)
			self.Lock.Lock()
			log.Println(quorum_log_info)
			delete(self.SDFSProgress, sdfs_filename)
			self.Lock.Unlock()

			return true
		}
	}
}

func (self *Slave) cal_quorum_num(num int) int {
	fmt.Println("****************** enter cal_quorum_num function **************")

	fmt.Println("quorum num is: " + strconv.Itoa(int(math.Ceil(float64((num+1)/2)))))
	return int(math.Ceil(float64((num + 1) / 2)))
}

func (self *Slave) Put_to_replica(replica_address string, local_filename string, sdfs_filename string,
	version int, wg *sync.WaitGroup) {
	defer wg.Done()
	// write to replica node
	cmd := exec.Command("bash")
	cmdWriter, _ := cmd.StdinPipe()
	err := cmd.Start()
	if err != nil {
		log.Fatal("error with writing to executing the bash command", err)
	}
	cmdString := fmt.Sprintf("sshpass -p %s scp %s %s@%s:%s",
		"XY19960218@uiuc", local_filename, "yaoxiao9", replica_address, SDFS_FILE_PREFIX+sdfs_filename)

	cmdWriter.Write([]byte(cmdString + "\n"))
	cmdWriter.Write([]byte("exit" + "\n"))

	cmd.Wait()

	host := net.JoinHostPort(replica_address, "9000")
	client, err := rpc.Dial("tcp", host)
	if err != nil {
		fmt.Println("error with connecting to replica node")
		log.Fatal("error with connecting to replica node", err)
	}
	defer client.Close()
	request := GetRequest{sdfs_filename, version}
	var null_response string
	null_response = ""
	err = client.Call("TCPServer.Update_file_version", request, &null_response)

	if !(strings.HasPrefix(local_filename, "/home/yaoxiao9/"+SDFS_FILE_PREFIX)) {
		fake_data := new(GetResponse)
		fake_data.Address = " "
		fake_data.Version = 1
		self.work_done(sdfs_filename, fake_data)
	}
}

func isFile_in_map(sdfs_filename string, SDFSProgress map[string][]*GetResponse) bool {
	for file, _ := range SDFSProgress {
		if sdfs_filename == file {
			return true
		}
	}
	return false
}

func (self *Slave) work_done(sdfs_filename string, data *GetResponse) {
	fmt.Println("****************** enter work_done function **************")
	self.Lock.Lock()
	if isFile_in_map(sdfs_filename, self.SDFSProgress) == true {
		self.SDFSProgress[sdfs_filename] = append(self.SDFSProgress[sdfs_filename], data)
	}
	self.Lock.Unlock()
}

func (self *Slave) Get_from_replica(target_ip string, sdfs_filename string, version int, wg *sync.WaitGroup) *GetResponse {
	fmt.Println("****************** enter Get_from_replica function **************")
	defer wg.Done()
	host := net.JoinHostPort(target_ip, "9000")
	client, err := rpc.Dial("tcp", host)
	if err != nil {
		log.Panic("error with connecting to master", err)
	}
	defer client.Close()
	request := GetRequest{sdfs_filename, version}

	get_response := new(GetResponse)
	err = client.Call("TCPServer.Get_file_data", request, get_response)
	fmt.Println(get_response.Address)
	fmt.Println(get_response.Version)
	self.work_done(sdfs_filename, get_response)
	return get_response
}

func (self *Slave) Get_file_data(sdfs_filename string, ver int) *GetResponse {
	fmt.Println("****************** enter Get_file_data function (Server) **************")
	local_ver := self.SDFSInfo.Get_file_version(sdfs_filename)
	fmt.Println("The LOCALVERSION is " + strconv.Itoa(local_ver))
	fmt.Println("The GETVERSION is " + strconv.Itoa(ver))
	res := new(GetResponse)
	if local_ver < ver {
		go self.Get(sdfs_filename, "/home/yaoxiao9/" + SDFS_FILE_PREFIX+sdfs_filename)
	}
	res.Version = local_ver
	res.Address = self.Address
	fmt.Println(res.Version)
	fmt.Println(res.Address)
	return res
}

func (self *Slave) Get(sdfs_filename string, local_filename string) {
	fmt.Println("****************** enter Get function **************")
	start_time := time.Now().Unix()
	logger.WriteLog("Contacting Master for getting file " + sdfs_filename)
	fmt.Println("Contacting Master for getting file" + sdfs_filename)
	host := net.JoinHostPort(self.master, "9000")
	client, err := rpc.Dial("tcp", host)
	if err != nil {
		log.Fatal("error with connecting to master", err)
	}
	defer client.Close()
	file_info := new(Got_File_Info)
	err = client.Call("TCPServer.Get_file_info", sdfs_filename, file_info)
	fmt.Println("length of address list: " + strconv.Itoa(len(file_info.Address_list)))
	fmt.Println(strconv.Itoa(file_info.Version))
	if len(file_info.Address_list) == 0 || file_info.Version == -1 {
		fmt.Println("No File Found for name" + sdfs_filename)
		logger.WriteLog("No File Found for name" + sdfs_filename)
		return
	}
	addreses := file_info.Address_list
	ver := file_info.Version
	fmt.Println("The file is of version " + strconv.Itoa(ver))
	self.InitWork(sdfs_filename)
	var wg sync.WaitGroup
	for _, address := range addreses {
		fmt.Println("Getting file from " + address)
		wg.Add(1)
		go self.Get_from_replica(address, sdfs_filename, ver, &wg)
	}
	wg.Wait()
	for {
		time.Sleep(HEARTBEAT_PERIOD)
		if len(self.SDFSProgress[sdfs_filename]) >= self.cal_quorum_num(len(addreses)) {
			fmt.Println("Quorum count:" + strconv.Itoa(len(self.SDFSProgress[sdfs_filename])))
			logger.WriteLog("Quorum count:" + strconv.Itoa(len(self.SDFSProgress[sdfs_filename])))
			break
		}
	}
	self.Lock.Lock()
	fmt.Println("The SDFSProgress list are")
	fmt.Println(self.SDFSProgress[sdfs_filename])
	for _, filemeta := range self.SDFSProgress[sdfs_filename] {
		fmt.Println("The Getting address are" + filemeta.Address)
		local_version := filemeta.Version
		fmt.Println("The local_version is ")
		fmt.Println(local_version)
		if local_version <= ver || len(self.SDFSProgress[sdfs_filename]) == 1 {
			cmd := exec.Command("bash")
			cmdWriter, _ := cmd.StdinPipe()
			err := cmd.Start()
			if err != nil {
				log.Fatal("error with writing to executing the bash command", err)
			}
			cmdString := fmt.Sprintf("sshpass -p %s scp %s@%s:%s %s",
				"XY19960218@uiuc", "yaoxiao9", filemeta.Address, SDFS_FILE_PREFIX+sdfs_filename, local_filename)

			cmdWriter.Write([]byte(cmdString + "\n"))
			cmdWriter.Write([]byte("exit" + "\n"))

			cmd.Wait()
			break
		}
	}
	delete(self.SDFSProgress, sdfs_filename)
	self.Lock.Unlock()
	if strings.HasPrefix(local_filename, "/home/yaoxiao9/"+SDFS_FILE_PREFIX) {
		self.SDFSInfo.Update_file_version(sdfs_filename, ver)
		logger.WriteLog("repair done for file " + sdfs_filename)
		fmt.Println("repair done for file " + sdfs_filename)
	} else {
		logger.WriteLog("write to local file " + local_filename)
		fmt.Println("write to local file " + local_filename)
		now := time.Now().Unix()
		logger.WriteLog("Done in " + strconv.FormatInt((now-start_time), 10) + "seconds")
		fmt.Println("Done in " + strconv.FormatInt((now-start_time), 10) + "seconds")
	}
}

func (self *Slave) ls(sdfs_filename string) {
	host := net.JoinHostPort(self.master, "9000")
	client, err := rpc.Dial("tcp", host)
	if err != nil {
		log.Fatal("error with connecting to master", err)
	}
	defer client.Close()
	file_info := new(Got_File_Info)
	err = client.Call("TCPServer.Get_file_info", sdfs_filename, file_info)
	if len(file_info.Address_list) == 0 || file_info.Version == -1 {
		fmt.Println("the file is not available!")
		log.Println("No File Found for name" + sdfs_filename)
		logger.WriteLog("No File Found for name" + sdfs_filename)
		return
	}
	addresses := file_info.Address_list
	version := file_info.Version
	logger.WriteLog("Getting File location of" + sdfs_filename + " with file version " + strconv.Itoa(version))
	log.Println("Getting File location of" + sdfs_filename + " with file version " + strconv.Itoa(version))
	for i, ip := range addresses {
		logger.WriteLog("Replica " + strconv.Itoa(i) + " the corresponding ip is :" + ip)
		fmt.Println("Replica " + strconv.Itoa(i) + " the corresponding ip is :" + ip)
	}
}

func (self *Slave) store() {
	files := self.SDFSInfo.Ls_file()
	if len(files) == 0 {
		fmt.Println("no files stored on this node")
	}
	for i, file := range files {
		logger.WriteLog("SDFS File " + strconv.Itoa(i) + " the file name is :" + file)
		fmt.Println("SDFS File " + strconv.Itoa(i) + " the file name is :" + file)
	}
}

func (self *Slave) revote_master() {
	if self.VoteStatus.Vote == false {
		self.VoteStatus.Vote = true
		self.VoteStatus.Vote_num = 0
		self.VoteStatus.Voters = make(map[string]int)
	}
	if self.Address == self.MemberList[0].Address {
		self.VoteStatus.Vote_num += 1
		return
	}
	host := net.JoinHostPort(self.MemberList[0].Address, "9000")
	client, err := rpc.Dial("tcp", host)
	if err != nil {
		log.Fatal("error with connecting to master", err)
	}
	defer client.Close()
	rep := new(string)
	client.Call("TCPServer.Vote", self.Address, rep)
}

func isStringinMap(voter string, voters map[string]int) bool {
	for vote, _ := range voters {
		if vote == voter {
			return true
		}
	}
	return false
}

func isStringinBigMap(address string, address_map map[string][]map[string]int) bool {
	for vote, _ := range address_map {
		if vote == address {
			return true
		}
	}
	return false
}

func (self *Slave) Receive_vote(voter string) {
	if self.VoteStatus.Vote == false {
		self.VoteStatus.Vote = true
		self.VoteStatus.Vote_num = 0
		self.VoteStatus.Voters = make(map[string]int)
	}
	if isStringinMap(voter, self.VoteStatus.Voters) == false {
		self.VoteStatus.Voters[voter] = 1
		self.VoteStatus.Vote_num += 1
	}
	if (self.master != self.Address) && (self.VoteStatus.Vote_num > (len(self.MemberList) / 2)) {
		self.master = self.Address
		logger.WriteLog("I'm selected as the new master")
		fmt.Println("I'm selected as the new master")
		go self.rebuild_file_meta()
	}
}

func (self *Slave) rebuild_file_meta() {
	time.Sleep(HEARTBEAT_PERIOD * 2)
	tmp_file_meta := make(map[string][]map[string]int)
	for _, member := range self.MemberList {
		var member_files map[string]int
		if member.Address == self.Address {
			member_files = self.SDFSInfo.Ls_localfile()
		} else {
			host := net.JoinHostPort(self.MemberList[0].Address, "9000")
			client, err := rpc.Dial("tcp", host)
			if err != nil {
				log.Fatal("error with connecting to master", err)
			}
			reply := new(map[string]int)
			err = client.Call("TCPServer.Assign_new_master", self.Address, reply)
			member_files = *reply
		}
		fmt.Print("the length of new member_files are: ")
		fmt.Println(len(member_files))
		for filename, version := range member_files {
			fmt.Println("filename:" + filename)
			fmt.Print("version: ")
			fmt.Println(version)
			if isStringinBigMap(filename, tmp_file_meta) == false {
				empty := make([]map[string]int, 0)
				tmp_file_meta[filename] = empty
			}
			new_map := make(map[string]int)
			new_map[member.Address] = version
			tmp_file_meta[filename] = append(tmp_file_meta[filename], new_map)
		}
	}
	fmt.Print("the new tmp_file_meta is: ")
	fmt.Println(len(tmp_file_meta))
	for file, file_info_list := range tmp_file_meta {
		sl := sortByValue(file_info_list)
		value := new(master.File_info)
		if len(sl) <= 4 {
			for index, _ := range sl {
				value.Node_list = append(value.Node_list, sl[index].Key)
			}
		} else {
			for index, _ := range sl {
				if index >= 4 {
					break
				}
				value.Node_list = append(value.Node_list, sl[index].Key)
			}
		}
		value.Version = sl[0].Value
		value.Timestamp = time.Now().UnixNano()
		self.SDFSMaster.Put_file_meta(file, value)
	}
	logger.WriteLog("SDFS file meta has been rebuilt")
	self.VoteStatus.Vote = false
	self.VoteStatus.Voters = make(map[string]int)
	go self.Fail_recover()
}

func (self *Slave) Assign_New_Master(new_master string) map[string]int {
	self.master = new_master
	self.VoteStatus.Vote = false
	logger.WriteLog("accepting new matser: " + (new_master))
	fmt.Println("accepting new matser: " + (new_master))
	return self.SDFSInfo.Ls_localfile()
}

func (self *Slave) Update_file_version(filename string, version int) {
	self.SDFSInfo.Update_file_version(filename, version)
}

func (self *Slave) delete(sdfs_filename string) {
	fmt.Println("*******Executing Delete Operation*******" + sdfs_filename)
	logger.WriteLog("*******Executing Delete Operation*******" + sdfs_filename)
	host := net.JoinHostPort(self.master, "9000")
	client, err := rpc.Dial("tcp", host)
	if err != nil {
		log.Fatal("error with connecting to master while deleting file", err)
	}
	defer client.Close()

	var replicas []string
	err = client.Call("TCPServer.Get_delete_info", sdfs_filename, &replicas)
	if len(replicas) == 0 {
		fmt.Println("the file is not available")
	}
	if err != nil {
		log.Fatal("error with getting deleting replica info", err)
	}
	for _, replica_address := range replicas {
		if replica_address == self.Address {
			self.SDFSInfo.Delete_file_data(SDFS_FILE_PREFIX + sdfs_filename)
		} else {
			rep_host := net.JoinHostPort(replica_address, "9000")
			rep_client, err := rpc.Dial("tcp", rep_host)
			var null_response string
			err = rep_client.Call("TCPServer.Delete_file_data", (SDFS_FILE_PREFIX + sdfs_filename), &null_response)
			if err != nil {
				log.Fatal("error with deleting file replicas", err)
			}
		}
	}
	logger_info := "deletion is done for" + sdfs_filename
	logger.WriteLog(logger_info)
	log.Print(logger_info)
}

func (self *Slave) Re_put(replica_address string, local_filename string, sdfs_filename string, version int) {
	// write to replica node
	cmd := exec.Command("bash")
	cmdWriter, _ := cmd.StdinPipe()
	err := cmd.Start()
	if err != nil {
		log.Fatal("error with writing to executing the bash command", err)
	}
	cmdString := fmt.Sprintf("sshpass -p %s scp %s %s@%s:%s",
		"XY19960218@uiuc", local_filename, "yaoxiao9", replica_address, SDFS_FILE_PREFIX+sdfs_filename)

	cmdWriter.Write([]byte(cmdString + "\n"))
	cmdWriter.Write([]byte("exit" + "\n"))

	cmd.Wait()

	host := net.JoinHostPort(replica_address, "9000")
	client, err := rpc.Dial("tcp", host)
	if err != nil {
		fmt.Println("error with connecting to replica node")
		log.Fatal("error with connecting to replica node", err)
	}
	defer client.Close()
	request := GetRequest{sdfs_filename, version}
	var null_response string
	err = client.Call("TCPServer.Update_file_version", request, &null_response)
}

func (self *Slave) Fail_recover() {
	time.Sleep(HEARTBEAT_PERIOD * 8)
	host := net.JoinHostPort(self.master, "9000")
	client_host, err := rpc.Dial("tcp", host)
	if err != nil {
		log.Fatal("error with connecting to master", err)
	}
	defer client_host.Close()
	update_meta := new(map[string]*master.Replicate_info)
	fmt.Println("*******Detect Failure for some replicas, recovering from it ********")
	self.SDFSMaster.Update_member(self.MemberList)
	err = client_host.Call("TCPServer.Get_Update_Meta", self.MemberList, update_meta)
	if err != nil {
		log.Fatal("error with calling method get update meta data", err)
	}
	fmt.Println("The Got Update_data is")
	fmt.Println(*update_meta)
	if len(*update_meta) == 0 {
		fmt.Println("*******Detect Failure for some replicas, recovering from it ********")
		return
	}
	for filename, meta := range *update_meta {
		good_node := meta.Node1
		ver := meta.Ver
		new_nodes := meta.New_node_list
		fmt.Println("The First Good Node is "+ good_node)
		fmt.Println("The File Version is "+ strconv.Itoa(ver))
		fmt.Println("The rep_list is ")
		fmt.Println(new_nodes)

		for _, address := range new_nodes {
			log_info := "Reparing " + filename + " to " + address
			logger.WriteLog(log_info)
			if good_node == self.Address {
				go self.Re_put(address, "/home/yaoxiao9/" + SDFS_FILE_PREFIX + filename, filename, ver)
			} else {
				host := net.JoinHostPort(good_node, "9000")
				client, err := rpc.Dial("tcp", host)
				if err != nil {
					log.Fatal("error with connecting to master while deleting file", err)
				}
				defer client.Close()
				fmt.Println("Sending request to update remote replica places with " + address)
				var null_response string
				request := Replica_Recover_request{address, "/home/yaoxiao9/"+ SDFS_FILE_PREFIX + filename, filename, ver}
				err = client.Call("TCPServer.Remote_put_to_replica", request, &null_response)
				if err != nil {
					log.Println(err)
				}
			}
		}
	}
	logger.WriteLog("Repair done")
}
