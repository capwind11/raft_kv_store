package main

import (
	"log"
	"raft_kv_store/raft"
	"time"
)

func initNode() []*raft.RaftNode {

	raftAddrs := []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003", "127.0.0.1:9004"}
	nodes := make([]*raft.RaftNode, 0)
	for i := 0; i < len(raftAddrs); i += 1 {
		raftNode := raft.NewRaftNode(raftAddrs[i], i, raftAddrs)
		nodes = append(nodes, raftNode)
		go func(id int) {
			_, err := nodes[id].RunRpcServer()
			if err != nil {
				log.Printf("[RaftNode.runRpcServer error]:%v\n", err)
			}
		}(i)
	}
	for i := 0; i < len(raftAddrs); i += 1 {
		go nodes[i].RunServer()
	}
	return nodes
}

func main() {

	//raftAddrs := []string{"127.0.0.1:9000",
	//	"127.0.0.1:9001", "127.0.0.1:9002",
	//	"127.0.0.1:9003", "127.0.0.1:9004"}
	initNode()
	for {
		time.Sleep(3 * time.Second)
	}
}
