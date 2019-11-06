package master

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

type File_info struct {
	Node_list []string
	Version   int
	Timestamp int64
}

type Member struct {
	Address        string
	HeartbeatCount int
	UpdateTime     int64
}

type SDFSMaster struct {
	File_matadata map[string]*File_info
	Member_list   []*Member
}

type Replicate_info struct {
	Node1         string
	Ver           int
	New_node_list []string
}

type Put_response struct {
	Ip_list []string
	Version int
}

func InitMaster() *SDFSMaster {
	var list1 []*Member
	new_master := new(SDFSMaster)
	new_master.File_matadata = make(map[string]*File_info)
	new_master.Member_list = list1
	return new_master
}

func (self *SDFSMaster) Update_member(member_list []*Member) {
	self.Member_list = member_list
}

func (self *SDFSMaster) Put_file_meta(filename string, information *File_info) {
	self.File_matadata[filename] = information
	fmt.Println("Put File DOWN: ")
	fmt.Println(self.File_matadata)
}

func getIndex(m string, s []string) int {
	for index, ss := range s {
		if m == ss {
			return index
		}
	}
	return -1
}

func isAddressExist(m string, s []string) bool {
	for _, ss := range s {
		if m == ss {
			return true
		}
	}
	return false
}

func (self *SDFSMaster) Update_metadata(memberlist []*Member) map[string]*Replicate_info {
	fmt.Println("***** Working on the function of Updating metadata (SDFSMaster)")
	var replicate map[string]*Replicate_info
	var available []string
	for _, item := range memberlist {
		if isAddressExist(item.Address, available) == false {
			available = append(available, item.Address)
		}
	}
	fmt.Println("The available List Now is")
	fmt.Println(available)
	fmt.Println("The File metadata now is:")
	fmt.Println(self.File_matadata)
	for filename, _ := range self.File_matadata {
		fmt.Println("The key is:")
		fmt.Println(filename)
		fmt.Println(self.File_matadata[filename])
		var down_nodes []string
		var working_nodes []string
		for _, node := range self.File_matadata[filename].Node_list {
			if isAddressExist(node, available) == true {
				working_nodes = append(working_nodes, node)
			} else {
				down_nodes = append(down_nodes, node)
			}
		}
		fmt.Println("The working List Now is")
		fmt.Println(working_nodes)
		fmt.Println("The Down List Now is")
		fmt.Println(down_nodes)
		if len(working_nodes) < 4 {
			ver := self.File_matadata[filename].Version
			self.File_matadata[filename].Node_list = working_nodes
			self.Init_replica(filename)
			list1 := self.File_matadata[filename].Node_list
			list2 := working_nodes
			var new_list []string
			for _, value := range list1 {
				if isAddressExist(value, list2) == false {
					new_list = append(new_list, value)
				}
			}
			fmt.Println("The Replica node List that need to be update is")
			fmt.Println(new_list)
			replicate = make(map[string]*Replicate_info)
			rep_info := new(Replicate_info)
			rep_info.Node1 = working_nodes[0]
			rep_info.New_node_list = new_list
			rep_info.Ver = ver
			replicate[filename] = rep_info
		}
	}
	return replicate
}

func (self *SDFSMaster) Init_replica(filename string) {
	for {
		if len(self.File_matadata[filename].Node_list) >= 4 {
			break
		}
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		num := r.Intn(len(self.Member_list) - 1)
		address := self.Member_list[num].Address
		if isAddressExist(address, self.File_matadata[filename].Node_list) == false {
			self.File_matadata[filename].Node_list = append(self.File_matadata[filename].Node_list, address)
		}
	}
	fmt.Println("now the file_metadata is:  ")
	for key, value := range self.File_matadata {
		fmt.Println("filename: " + key)
		fmt.Println("node list is: ")
		for _, node := range value.Node_list {
			fmt.Print(node + ", ")
		}
	}
}

func (self *SDFSMaster) Handle_put_request(filename string) Put_response {
	var response Put_response
	self.Update_timestamp(filename)
	self.Init_replica(filename)
	fmt.Println("filename ready to put into file_meatadata: " + filename)
	fmt.Println("the version is :" + strconv.Itoa(self.File_matadata[filename].Version+1))

	self.File_matadata[filename].Version = self.File_matadata[filename].Version + 1
	response.Ip_list = self.File_matadata[filename].Node_list
	response.Version = self.File_matadata[filename].Version

	fmt.Println("now the file_metadata is:  ")
	for key, value := range self.File_matadata {
		fmt.Println("filename: " + key)
		fmt.Println("node list is: ")
		for _, node := range value.Node_list {
			fmt.Print(node + ", ")
		}
		fmt.Println("version is: " + strconv.Itoa(value.Version))

	}

	return response
}

func (self *SDFSMaster) Get_file_version(filename string) int {
	fmt.Println("****************** enter Get_file_version(master) function **************")
	for file, _ := range self.File_matadata {
		if filename == file {
			return self.File_matadata[filename].Version
		}
	}
	return -1
}

func (self *SDFSMaster) Get_file_timestamp(filename string) int64 {
	for file, _ := range self.File_matadata {
		if filename == file {
			return self.File_matadata[filename].Timestamp
		}
	}
	return -1
}

func (self *SDFSMaster) Get_file_replica_list(filename string) []string {
	fmt.Println("****************** enter Get_file_replica_list(master) function **************")
	var null_list []string
	fmt.Println("length of file_metadata: " + strconv.Itoa(len(self.File_matadata)))
	for file, _ := range self.File_matadata {
		fmt.Println("files already stored: " + file)
		fmt.Println("enter loop: "+filename == file)
		fmt.Println("length of filename input: " + filename + "," + strconv.Itoa(len(filename)))
		fmt.Println("length of file in metadata: " + file + "," + strconv.Itoa(len(file)))

		if filename == file {
			return self.File_matadata[filename].Node_list
		}
	}
	return null_list
}

func (self *SDFSMaster) If_file_updated_recent(filename string) bool {
	fmt.Println("********* enter If_file_updated_recent(master) ************")
	for file, _ := range self.File_matadata {
		fmt.Println("enter for loop")
		if filename == file {
			fmt.Println("enter if condition")
			time := time.Now().UnixNano()
			last_update := self.Get_file_timestamp(filename)
			fmt.Print("last update time is: ")
			fmt.Println(last_update)
			fmt.Println(time-last_update < 60000000000)
			return time-last_update < 60000000000
		}
	}
	return false
}

func (self *SDFSMaster) Update_timestamp(filename string) {
	for file, _ := range self.File_matadata {
		if filename == file {
			time := time.Now().UnixNano()
			self.File_matadata[filename].Timestamp = time
			return
		}
	}
	new_Info := new(File_info)
	var new_list []string
	new_Info.Node_list = new_list
	time := time.Now().UnixNano()
	new_Info.Timestamp = time
	new_Info.Version = 0
	self.File_matadata[filename] = new_Info
	return
}

func (self *SDFSMaster) Delete_file_info(filename string) []string {
	var empty []string
	for file, _ := range self.File_matadata {
		if filename == file {
			old_nodes := self.File_matadata[filename].Node_list
			delete(self.File_matadata, file)
			return old_nodes
		}
	}
	return empty
}
