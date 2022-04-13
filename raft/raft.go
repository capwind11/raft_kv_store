package raft

import (
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type RaftNode struct {
	// 基础属性
	peers         []*RpcServer
	nodeID        int
	currentTerm   int64
	heartBeatChan chan interface{}
	listener      net.Listener
	rpcServer     *rpc.Server
	mutexLock     sync.Mutex

	// leader选举相关
	status       RaftStatus
	leaderID     int
	voteFor      int
	electionChan chan bool

	// 从节点日志复制相关
	// TODO
	heartBeatIntervalTime time.Duration // 心跳间隔时间
}

type requestVoteResult struct {
	voteResponse *RequestVoteResponse
}

func NewRaftNode(addr string, nodeID int, peers []string) *RaftNode {
	raftNode := &RaftNode{
		nodeID:      nodeID,
		currentTerm: 0,
		status:      RAFT_FOLLOWER,
		voteFor:     NOVOTE,
		leaderID:    UNKNOW_LEADER,
		peers:       make([]*RpcServer, 0),
		// 日志复制相关属性
		heartBeatIntervalTime: 200 * time.Millisecond,
	}

	for _, peer := range peers {
		raftNode.peers = append(raftNode.peers, NewRpcServer(peer))
	}
	raftNode.heartBeatChan = make(chan interface{}, len(raftNode.peers))
	raftNode.electionChan = make(chan bool, 1)
	return raftNode
}

func (r *RaftNode) reset() {
	r.currentTerm = 0
	r.status = RAFT_FOLLOWER
	r.voteFor = NOVOTE
	r.leaderID = UNKNOW_LEADER
}

func (r *RaftNode) RunRpcServer() (net.Listener, error) {
	server := rpc.NewServer()
	server.Register(r)
	r.rpcServer = server
	lis, err := net.Listen("tcp", r.peers[r.nodeID].address)
	r.listener = lis
	if err != nil {
		return nil, err
	}
	return lis, http.Serve(lis, server)
}

func (r *RaftNode) closeRpcServer() error {
	if err := r.listener.Close(); err != nil {
		return err
	}
	return nil
}

func (r *RaftNode) RunServer() {
	// 执行日志同步

	for {
		switch r.status {
		case RAFT_LEADER:
			select {
			case <-time.After(r.heartBeatIntervalTime):
				// sendHeartBeat
				r.sendHeartBeat()
			}
		case RAFT_CANDIDATES:
			go r.election()
			select {
			case <-r.heartBeatChan:
				r.status = RAFT_FOLLOWER
			case success := <-r.electionChan:
				if success {
					r.status = RAFT_LEADER
					log.Printf("[election successful]: leaderID:[%d],term:[%d]\n", r.nodeID, r.currentTerm)
				} else {
					r.status = RAFT_FOLLOWER
					log.Printf("[election fail]: candidateID:[%d]", r.nodeID)
				}
			case <-time.After(5*time.Second + time.Duration(rand.Intn(5000))*time.Millisecond): //超时成为从节点
				r.status = RAFT_FOLLOWER
				log.Printf("[election timeout]: nodeID:[%d], term:[%d]", r.nodeID, r.currentTerm)
			}
		case RAFT_FOLLOWER:
			select {
			case <-r.heartBeatChan:
				r.status = RAFT_FOLLOWER
			case <-time.After(time.Duration(250+rand.Intn(500)) * time.Millisecond):
				r.status = RAFT_CANDIDATES
			}
		}
	}
}

func (r *RaftNode) election() {
	if r.status != RAFT_CANDIDATES {
		return
	}

	r.mutexLock.Lock()
	r.status = RAFT_CANDIDATES
	r.currentTerm += 1
	r.voteFor = r.nodeID
	r.leaderID = UNKNOW_LEADER
	log.Printf("[launch an election] candidateID:[%v], term:[%v]\n", r.nodeID, r.currentTerm)
	r.mutexLock.Unlock()

	voteReq := &RequestVoteRequest{
		Term:        r.currentTerm,
		CandidateID: r.nodeID,
	}
	voteResultChan := make(chan *requestVoteResult, len(r.peers)-1)

	for peerID := 0; peerID < len(r.peers); peerID += 1 {
		if peerID == r.nodeID {
			continue
		}
		go func(id int) {
			voteResp := &RequestVoteResponse{}
			if err := r.peers[id].Call("RaftNode.RequestVote", voteReq, voteResp); err == nil {
				voteResultChan <- &requestVoteResult{voteResponse: voteResp}
			} else {
				log.Printf("[RequestVote error]:%v\n]", err)
				voteResultChan <- &requestVoteResult{voteResponse: nil}
			}
		}(peerID)
	}

	voteCount, total := 1, 1
	maxTerm := r.currentTerm
	for {
		select {
		case voteResult := <-voteResultChan:
			total += 1
			if voteResult.voteResponse == nil {
				break
			}
			if voteResult.voteResponse.IsVoteFor {
				voteCount += 1
			}
			if maxTerm < voteResult.voteResponse.Term {
				maxTerm = voteResult.voteResponse.Term
			}
		}
		if total == len(r.peers) || voteCount >= (len(r.peers)+1)/2 {
			break
		}
	}
	log.Printf("[election]: nodeID:[%d], term:[%d], voteCount:[%d], total:[%d]\n", r.nodeID, r.currentTerm, voteCount, total)
	// 分析投票结果
	r.mutexLock.Lock()
	defer func() {

		r.mutexLock.Unlock()
	}()
	if r.status != RAFT_CANDIDATES {
		r.electionChan <- false
		return
	}
	if maxTerm > r.currentTerm {
		r.status = RAFT_FOLLOWER
		r.currentTerm = maxTerm
		r.voteFor = NOVOTE
		r.electionChan <- false
		return
	}
	if voteCount >= (len(r.peers)+1)/2 {
		r.status = RAFT_LEADER
		r.leaderID = r.nodeID
		r.electionChan <- true
		r.sendHeartBeat()
	} else {
		r.electionChan <- false
	}

}

func (r *RaftNode) sendHeartBeat() {
	if r.status != RAFT_LEADER {
		return
	}

	heartBeatReq := &HeartBeatReq{
		Term:     r.currentTerm,
		LeaderID: r.nodeID,
	}
	for i := 0; i < len(r.peers); i += 1 {
		if i == r.nodeID {
			continue
		}
		go func(id int) {
			heartBeatResp := &HeartBeatResp{}
			err := r.peers[id].Call("RaftNode.AcceptHeartBeat",
				heartBeatReq, heartBeatResp)
			if err != nil {
				log.Printf("[sendHeartBeat error]:%v\n", err)
				return
			}
		}(i)
	}
}
