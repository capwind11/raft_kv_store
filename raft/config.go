package raft

type RaftStatus int

// RAFT ROLES
const (
	RAFT_LEADER     RaftStatus = 1
	RAFT_CANDIDATES RaftStatus = 2
	RAFT_FOLLOWER   RaftStatus = 3
)

// NODE STATUS
const (
	NOVOTE        int   = 1
	UNKNOW_LEADER int   = -1
	UNKNOW_TERM   int64 = -1
)

// ERROR
const (
	BAD_REQUEST           int = 400
	INTERNAL_SERVER_ERROR int = 500
)
