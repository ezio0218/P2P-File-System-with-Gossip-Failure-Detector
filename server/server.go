package server

import (
	"../logger"
	"../master"
	"../slave"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TCPServer struct {
	slave      *slave.Slave
	SDFSMaster *master.SDFSMaster
	UDPConn    *net.UDPConn
}

type Replica_Recover_request struct {
	replica_address string
	local_filename  string
	sdfs_filename   string
	version         int
	wg              *sync.WaitGroup
}

type ClientRequest struct {
	Greppattern string
	LogFile     string
}

type Confirmation struct {
	Command string
	Time    time.Time
}

func InitTCPServer(mas *master.SDFSMaster, sla *slave.Slave) *TCPServer {
	new_TCPServer := new(TCPServer)
	new_TCPServer.slave = sla
	new_TCPServer.SDFSMaster = mas

	return new_TCPServer
}

func showCmd(cmd *exec.Cmd) {
	fmt.Printf("==> Executing: %s\n", strings.Join(cmd.Args, " "))
}

func (self *TCPServer) Response(query ClientRequest, res *string) error {
	err := os.Chdir("/home/yaoxiao9/ourmp1/logfile/")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	name := "grep"
	fmt.Println("Current Log File: ", query.LogFile)
	cmd := exec.Command(name, "-c", query.Greppattern, query.LogFile)
	fmt.Println(query.Greppattern)
	showCmd(cmd)
	out, error := cmd.Output()
	if error != nil {
		fmt.Println("Command Fails", error)
	}
	*res = strings.TrimSpace(string(out))
	return nil
}

func (self *TCPServer) Get_put_info(request slave.PutRequest, response *slave.Put_response_info) error {
	sdfs_filename := request.SDFSFilename
	requester_address := request.Adress
	start_time := time.Now()
	//response := new(Put_response_info)
	if self.SDFSMaster.If_file_updated_recent(sdfs_filename) {
		logger.WriteLog("Write-Write conflicts detected")
		fmt.Println("Write-Write conflicts detected", time.Now().Sub(start_time))
		// ask for confirmation
		if requester_address == self.slave.Address {
			fmt.Println("requester is master")
			confirmation_response := ask_for_confirmation()
			if !(confirmation_response.Command == "yes" && confirmation_response.Time.Sub(start_time).Seconds() < 30) {
				response.Put_or_not = false
			} else {
				response.Put_or_not = true
				response.Response = self.SDFSMaster.Handle_put_request(sdfs_filename)
			}
		} else {
			host := net.JoinHostPort(requester_address, "9000")
			client, err := rpc.Dial("tcp", host)
			if err != nil {
				log.Fatal("error with connecting to put_requester", err)
				return err
			}
			defer client.Close()
			confirmation_response := new(Confirmation)
			new_string := "sss"
			err = client.Call("TCPServer.Ask_for_confirmation", new_string, confirmation_response)
			if err != nil {
				log.Fatal("error with asking for put-conflict confirmation", err)
				return err
			}
			if !(confirmation_response.Command == "yes" && confirmation_response.Time.Sub(start_time).Seconds() < 30) {
				fmt.Println("write-write conflicts, reject update")
				response.Put_or_not = false
			} else {
				response.Put_or_not = true
				response.Response = self.SDFSMaster.Handle_put_request(sdfs_filename)
			}
		}
	} else {
		response.Put_or_not = true
		response.Response = self.SDFSMaster.Handle_put_request(sdfs_filename)
	}

	return nil
}

func (self *TCPServer) Get_file_data(request slave.GetRequest, response *slave.GetResponse) error {
	fmt.Println("Execting Get_file_data (Server)")
	r := self.slave.Get_file_data(request.SDFSFilename, request.Version)
	response.Version = r.Version
	response.Address = r.Address
	fmt.Println(r.Address)
	fmt.Println(r.Version)
	return nil
}

func (self *TCPServer) Get_file_info(sdfsfilename string, res *slave.Got_File_Info) error {
	fmt.Println("****************** enter Get_file_info(server) function **************")
	res.Address_list = self.SDFSMaster.Get_file_replica_list(sdfsfilename)
	res.Version = self.SDFSMaster.Get_file_version(sdfsfilename)
	for i, _ := range res.Address_list {
		fmt.Println("The address list got in Tcp Server side is " + res.Address_list[i])
	}
	fmt.Println("The address list Version got in Tcp Server side is " + strconv.Itoa(res.Version))
	return nil
}

func ask_for_confirmation() *Confirmation {
	fmt.Println("The file has just been updated, still proceed? (yes/no) ")
	var input string
	fmt.Scanln(&input)
	confirmation := new(Confirmation)
	confirmation.Command = input
	confirmation.Time = time.Now()

	return confirmation
}

func (self *TCPServer) Ask_for_confirmation(null string, confirmation *Confirmation) error {
	fmt.Println("The file has just been updated, still proceed? (yes/no) ")

	ch := make(chan string)
	var input string
	go func() {
		time.Sleep(time.Second * 2)
		fmt.Scanln(&input)
		ch <- input
	}()

	select {
	case res := <-ch:
		fmt.Println("your input is: " + res)
		confirmation.Command = input
		confirmation.Time = time.Now()
		return nil
	case <-time.After(time.Second * 30):
		fmt.Println("input timeout, no response")
		confirmation.Command = "no"
		return nil
	}
}

func (self *TCPServer) StartTCP() {
	rpc.Register(self) //??????????/ self or &self
	address, error := net.ResolveTCPAddr("tcp", ":9000")
	if error != nil {
		log.Fatal("Address resolving Error: ", error)
		os.Exit(1)
	}
	listener, error := net.ListenTCP("tcp", address)
	fmt.Println("Server started")
	if error != nil {
		log.Fatal("Listening establishment Error: ", error)
		os.Exit(1)
	}
	for {
		conn, error := listener.Accept()
		if error != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}

//func (self *TCPServer)startUDP() {
//	udpaddress, err := net.ResolveUDPAddr("udp", self.slave.Address)
//	if err != nil {
//		log.Fatal("Bind UDP Address error", err)
//		return
//	}
//	conn, err := net.ListenUDP("udp", udpaddress)
//	if err != nil {
//		return
//	}
//	self.UDPConn = conn
//}

func (self *TCPServer) Get_delete_info(sdfs_filename string, replicas *[]string) error {

	*replicas = self.SDFSMaster.Delete_file_info(sdfs_filename)

	return nil
}

func (self *TCPServer) Delete_file_data(sdfs_filename string, null_string *string) error {
	return self.slave.SDFSInfo.Delete_file_data(sdfs_filename)
}

func (self *TCPServer) Remote_reput(request slave.Replica_Recover_request, null_string *string) error {
	self.slave.Re_put(request.Replica_address, request.Local_filename, request.Sdfs_filename, request.Version)
	return nil
}

func (self *TCPServer) Vote(address string, reply *string) error {
	self.slave.Receive_vote(address)
	return nil
}

func (self *TCPServer) Assign_new_master(address string, reply *map[string]int) error {
	*reply = self.slave.Assign_New_Master(address)
	return nil
}

func (self *TCPServer) Update_file_version(request slave.GetRequest, null_string *string) error {
	fmt.Println("******** enter Update_file_version(server) *******")
	self.slave.SDFSInfo.Update_file_version(request.SDFSFilename, request.Version)
	return nil
}

func (self *TCPServer) Get_Update_Meta(memberlist []*master.Member, res *(map[string]*master.Replicate_info)) error {
	fmt.Println("******** enter Get_Update_Meta(server) *******")
	*res = self.SDFSMaster.Update_metadata(memberlist)
	return nil
}

//func main()  {
//	server := new(TCPServer)
//	rpc.Register(server)
//	address, error := net.ResolveTCPAddr("tcp", server.slave.Address)
//	if error != nil {
//		log.Fatal("Address resolving Error: ", error)
//		os.Exit(1)
//	}
//	listener, error := net.ListenTCP("tcp", address)
//	fmt.Println("Server started")
//	if error != nil {
//		log.Fatal("Listening establishment Error: ", error)
//		os.Exit(1)
//	}
//
//	for {
//		conn, error := listener.Accept()
//		if error != nil {
//			continue
//		}
//		go rpc.ServeConn(conn)
//	}
//
//}
