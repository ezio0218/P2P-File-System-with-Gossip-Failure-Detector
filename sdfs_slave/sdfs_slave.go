package sdfs_slave

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

type SDFSSLAVE struct {
	Local_files map[string]int
}

func Init_SDFSSlave() *SDFSSLAVE {
	new_slave := new(SDFSSLAVE)
	new_slave.Local_files = make(map[string]int)
	return new_slave
}

func (self *SDFSSLAVE) Update_file_version(filename string, version int) {
	fmt.Println("****************** enter Update_file_version(slave) function **************")

	self.Local_files[filename] = version
	fmt.Println("update file version(sdfs_slave): " + strconv.Itoa(self.Local_files[filename]))
}

func (self *SDFSSLAVE) Put_file(filename string, filedata []byte, version int) {
	self.Update_file_version(filename, version)
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	//defer to close when you're done with it, not because you think it's idiomatic!
	defer f.Close()
	f.Write(filedata)

}

func (self *SDFSSLAVE) Get_file_version(filename string) int {
	fmt.Println("Get_file_version(sdfs_slave): " + strconv.Itoa(self.Local_files[filename]))
	return self.Local_files[filename]
}

func (self *SDFSSLAVE) get_file(filename string, version int) (int, []byte) {
	if self.Local_files[filename] != version {
		return self.Local_files[filename], nil
	}
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	//defer to close when you're done with it, not because you think it's idiomatic!
	defer f.Close()
	buf := make([]byte, 4096)
	f.Read(buf)
	return self.Local_files[filename], buf
}

func (self *SDFSSLAVE) Ls_file() []string {
	var file_list []string
	for file, _ := range self.Local_files {
		file_list = append(file_list, file)
	}
	return file_list
}

func (self *SDFSSLAVE) Ls_localfile() map[string]int {
	return self.Local_files
}

func (self *SDFSSLAVE) Delete_file_data(sdfs_filename string) error {
	delete(self.Local_files, sdfs_filename)
	err := os.Remove("/home/yaoxiao9/" + sdfs_filename)
	if err != nil {
		fmt.Println("file remove Error!" + sdfs_filename)
		fmt.Printf("%s", err)
		return err
	} else {
		fmt.Print("file remove OK!" + sdfs_filename)
	}
	return nil
}
