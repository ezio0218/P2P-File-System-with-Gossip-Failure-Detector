# P2P-File-System-with-Gossip-Failure-Detector


on each vm, run the following command:

go build main.go go run main.go

And type "join" for join

"leave" for leave

"lsm" to check member

"IP" to check local IP and port number

"put \n local_filename sdfs_filename" to put local file into system as sdfs_filename

"get \n sdfs_filename local_filename" to get sdfs_filename from system and save to local as local_filename

"delete \n sdfs_filename" to delete sdfs_filename info from system"

"ls \n sdfs_filename" to get all the store nodes' IP address of given sdfs_filename

"store" to list all the sdfs files stored on the operating node

CTRL+C to crash node
