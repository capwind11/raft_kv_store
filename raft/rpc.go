package raft

import (
	"log"
	"net/rpc"
)

type RpcServer struct {
	address string
}

func (s *RpcServer) Call(method string, req interface{}, resp interface{}) (err error) {
	var client *rpc.Client
	if client, err = rpc.DialHTTP("tcp", s.address); err != nil {
		return err
	}
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
		client.Close()
	}()
	return client.Call(method, req, resp)
}

type RequestVoteRequest struct {
	Term        int64
	CandidateID int
}

type RequestVoteResponse struct {
	Term      int64
	IsVoteFor bool
}

func NewRpcServer(addr string) *RpcServer {
	return &RpcServer{
		address: addr,
	}
}

type HeartBeatReq struct {
	Term     int64
	LeaderID int
}

type HeartBeatResp struct {
	Term    int64
	Success bool
}

func (r *RaftNode) AcceptHeartBeat(req *HeartBeatReq, resp *HeartBeatResp) (err error) {

	r.mutexLock.Lock()
	defer r.mutexLock.Unlock()

	resp.Term = r.currentTerm
	resp.Success = false

	if req.Term < r.currentTerm {
		return
	}

	if req.Term >= r.currentTerm {
		r.currentTerm = req.Term
		r.status = RAFT_FOLLOWER
	}

	r.leaderID = req.LeaderID

	// 检查日志是否残缺
	r.heartBeatChan <- struct{}{}
	resp.Success = true
	return
}

func (r *RaftNode) RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) (err error) {

	r.mutexLock.Lock()
	defer r.mutexLock.Unlock()

	resp.Term = r.currentTerm
	resp.IsVoteFor = false

	if r.status != RAFT_FOLLOWER {
		return
	}

	if req.Term < r.currentTerm {
		return
	}

	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.status = RAFT_FOLLOWER
		r.voteFor = NOVOTE
	}

	if r.voteFor == NOVOTE {
		r.voteFor = req.CandidateID
		resp.IsVoteFor = true
		log.Printf("[vote] nodeID:[%v] vote for candidateID:[%v]\n", r.nodeID, req.CandidateID)
	}
	return
}
