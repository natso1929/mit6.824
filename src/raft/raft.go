package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Follower = iota
	Candidator
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	Logs        []*Logs

	commitIndex int
	lastApplied int

	// For Leader
	nextIdx  []int
	matchidx []int

	heartbeatTimer time.Time
	//electionTimeOut time.Duration
	state int
	//For all servers
	applyC        chan ApplyMsg
	commitedSigal chan struct{}
}

type Logs struct {
	Command interface{}
	Term    int
}

func (l *Logs) String() string {
	return fmt.Sprintf("{Command:%v,Term:%v}", l.Command, l.Term)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if e.Encode(rf.currentTerm) != nil {
		rf.dlog("...gob en_code 2 failed")
	}
	if e.Encode(rf.voteFor) != nil {
		rf.dlog("...gob en_code 3 failed")
	}
	if e.Encode(rf.Logs) != nil {
		rf.dlog("...gob en_code 1 failed")
	}
	//rf.dlog("...rf.currentTerm:%d, rf.voteFor:%d", rf.currentTerm, rf.voteFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []*Logs
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if d.Decode(&currentTerm) != nil {
		rf.dlog("...gob de_code 1 failed")
	}
	if d.Decode(&voteFor) != nil {
		rf.dlog("...gob de_code 2 failed")
	}
	if d.Decode(&logs) != nil {
		rf.dlog("...gob de_code 3 failed")

	} else {
		rf.Logs = logs
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
	}
	rf.dlog("...currentTerm:%v voteFor:%v", currentTerm, voteFor)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// heartbeat and sync request
	Term         int
	LeaderId     int
	LeaderCommit int

	PrevLogIdx  int
	PrevLogTerm int
	Entries     []*Logs
}

func (a *AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%v, LeaderId:%v, LeaderCommit:%v, PrevLogIdx:%v, PrevLogTerm:%v, Entries:%v}",
		a.Term, a.LeaderId, a.LeaderCommit, a.PrevLogIdx, a.PrevLogTerm, a.Entries)
}

type AppendEntriesReply struct {
	Term            int
	Success         bool
	FirstConfiltIdx int
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fromTerm := args.Term
	fromId := args.CandidateId
	defer rf.mu.Unlock()
	rf.mu.Lock()
	if rf.killed() {
		return
	}
	lastLogIndex := args.LastLogIndex
	lastLogTerm := args.LastLogTerm
	rf.dlog("...request from %d fromTerm:%d curTerm:%d", fromId, fromTerm, rf.currentTerm)
	if fromTerm > rf.currentTerm {
		rf.becomeFollower(fromTerm)
	}

	if fromTerm == rf.currentTerm {
		rf.dlog("...server-%d cur voteFor:%d state:%d", rf.me, rf.voteFor, rf.state)
		if rf.voteFor == -1 {
			if len(rf.Logs) == 0 || lastLogTerm > rf.Logs[len(rf.Logs)-1].Term {
				reply.VoteGranted, reply.Term = true, rf.currentTerm
				rf.voteFor = fromId
				rf.heartbeatTimer = time.Now()
				rf.dlog("...server-%d voteFor:%d state:%d successfully cur logs:%v", rf.me, rf.voteFor, rf.state, rf.Logs)
			} else if lastLogTerm < rf.Logs[len(rf.Logs)-1].Term {
				reply.VoteGranted, reply.Term = false, rf.currentTerm
				rf.dlog("...server-%d refuse voteFor:%d cur state:%d causeof curLastLogTerm bigger cur logs:%v", rf.me, fromId, rf.state, rf.Logs)
			} else {
				if lastLogIndex >= len(rf.Logs) {
					reply.VoteGranted, reply.Term = true, rf.currentTerm
					rf.voteFor = fromId
					rf.heartbeatTimer = time.Now()
					rf.dlog("...server-%d voteFor:%d state:%d successfully cur logs:%v", rf.me, rf.voteFor, rf.state, rf.Logs)
				} else {
					reply.VoteGranted, reply.Term = false, rf.currentTerm
					rf.dlog("...server-%d refuse voteFor:%d cur state:%d causeof curLastLogIndex bigger cur logs:%v", rf.me, fromId, rf.state, rf.Logs)
				}
			}
		} else if rf.voteFor == fromId {
			reply.VoteGranted, reply.Term = true, rf.currentTerm
			rf.voteFor = fromId
			rf.heartbeatTimer = time.Now()
			rf.dlog("...server-%d voteFor:%d repeatly state:%d cur logs:%v", rf.me, rf.voteFor, rf.state, rf.Logs)
		} else {
			reply.VoteGranted, reply.Term = false, rf.currentTerm
			rf.dlog("...server-%d voteFor:%d state:%d cur logs:%v\"", rf.me, rf.voteFor, rf.state, rf.Logs)
		}
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
	rf.dlog("...here 1")
	go rf.persist()
}

func (rf *Raft) HeartBeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	leaderTerm := args.Term
	leaderId := args.LeaderId
	//leaderCommit := args.LeaderCommit
	//logs := args.Entries
	//preLogTerm := args.PrevLogTerm
	//preLogIdx := args.PrevLogIdx

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if leaderTerm > rf.currentTerm {
		rf.dlog("...recives leader %d heartbeat", leaderId)
		rf.becomeFollower(leaderTerm)
		rf.heartbeatTimer = time.Now()
	} else if leaderTerm == rf.currentTerm {
		if rf.state == Leader {
			return
		}
		rf.becomeFollower(leaderTerm)
		rf.heartbeatTimer = time.Now()
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	leaderTerm := args.Term
	leaderId := args.LeaderId
	leaderCommit := args.LeaderCommit
	logs := args.Entries
	preLogTerm := args.PrevLogTerm
	preLogIdx := args.PrevLogIdx

	reply.FirstConfiltIdx = -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.dlog("...receive leader sync request %v", args)
	//heartbeat
	//if logs == nil {
	//	if leaderTerm >= rf.currentTerm {
	//		rf.dlog("...recives leader %d heartbeat", leaderId)
	//		rf.becomeFollower(leaderTerm)
	//		rf.heartbeatTimer = time.Now()
	//		// may exist problem
	//		rf.dlog("...heartbeat leaderCommit:%d server %d commitIdx:%d server.logs %v", leaderCommit, rf.me, rf.commitIndex, rf.Logs)
	//		//if leaderCommit > rf.commitIndex {
	//		//	for i := rf.commitIndex + 1; i < leaderCommit; i++ {
	//		//
	//		//	}
	//		//	rf.commitIndex = min(leaderCommit, len(rf.Logs))
	//		//	rf.commitedSigal <- struct{}{}
	//		//	rf.dlog("...server %d submit commitIdx %d", rf.me, rf.commitIndex)
	//		//}
	//	}
	//	return
	//}

	// AppendEntriesRPC & heartbeat
	//rf.dlog("...leader %d, CommitIdx:%d server %d commitIdx:%d server.logs %v", leaderId, leaderCommit, rf.me, rf.commitIndex, rf.Logs)
	rf.dlog("...requestSync Logs from leader %d", leaderId)
	if leaderTerm > rf.currentTerm {
		rf.becomeFollower(leaderTerm)
	}

	if leaderTerm == rf.currentTerm {
		if rf.state != Follower {
			rf.becomeFollower(leaderTerm)
		}
		rf.heartbeatTimer = time.Now()

		if preLogIdx == 0 || (preLogIdx <= len(rf.Logs) && preLogTerm == rf.Logs[preLogIdx-1].Term) {
			reply.Success, reply.Term = true, rf.currentTerm
			rf.Logs = rf.Logs[:preLogIdx]
			rf.Logs = append(rf.Logs, logs...)
			if len(rf.Logs) > preLogIdx+len(logs) {
				remain := rf.Logs[preLogIdx+len(logs)+1:]
				rf.Logs = append(rf.Logs, remain...)
			}

			rf.dlog("...server %d recieve sync logs and heartbeat preLogIdx:%d, preLogTerm:%d, cur len(logs):%v rf.logs: %v", rf.me, preLogIdx, preLogTerm, len(rf.Logs),
				rf.Logs)
			if leaderCommit > rf.commitIndex {
				if rf.killed() {
					return
				}
				rf.commitIndex = min(leaderCommit, len(rf.Logs))
				rf.dlog("...leaderCommit:%d, rf.commitIdx:%v", leaderCommit, rf.commitIndex)
				rf.dlog("...server %d submit commitIdx %d", rf.me, rf.commitIndex)
				rf.commitedSigal <- struct{}{}
			}
		} else {
			var firstConflictIdx int
			if preLogIdx > len(rf.Logs) {
				firstConflictIdx = len(rf.Logs)
			} else {
				firstConflictIdx = preLogIdx
			}
			for firstConflictIdx > 0 && rf.Logs[firstConflictIdx-1].Term != preLogTerm {
				firstConflictIdx -= 1
			}
			reply.Success, reply.Term, reply.FirstConfiltIdx = false, rf.currentTerm, firstConflictIdx+1
		}
	} else {
		reply.Success, reply.Term = false, rf.currentTerm
	}
	rf.dlog("...here 2")
	go rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.Logs) + 1
	if !isLeader {
		return -1, -1, false
	}
	// Your code here (2B).
	rf.dlog("...leader get cliend sync log request command %v term %v index %v", command, rf.currentTerm, rf.commitIndex)
	rf.Logs = append(rf.Logs, &Logs{command, term})
	go rf.persist()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.commitedSigal)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func getElectionTimeOut() time.Duration {
	interval := 350 + rand.Intn(200)
	return time.Duration(interval) * time.Millisecond
}

func (rf *Raft) becomeFollower(term int) {
	// follower
	//rf.mu.Lock()
	if rf.killed() {
		return
	}
	rf.state = Follower
	rf.voteFor = -1
	rf.currentTerm = term
	//rf.mu.Unlock()
	rf.dlog("...become Follower")
	go rf.becomeCandidate()
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	//curTerm := rf.currentTerm
	candidateTimer := getElectionTimeOut()
	rf.mu.Unlock()
	for {
		select {
		case <-time.After(10 * time.Millisecond):
			rf.mu.Lock()
			// 这里存在导致一个节点重复竞选， 其他节点不断被中断；
			//if curTerm != rf.currentTerm {
			//	rf.dlog("...server %d meet bigger term", rf.me)
			//	rf.mu.Unlock()
			//	return
			//}
			if rf.state == Leader {
				rf.mu.Unlock()
				return
			}
			if elasped := time.Since(rf.heartbeatTimer); elasped >= candidateTimer {
				//rf.dlog("...rf.heartbeatTimer:%d", rf.heartbeatTimer)
				rf.dlog("...elasped:%d > electionTimeOut:%d", elasped, candidateTimer)
				rf.dlog("...become Candidate")
				rf.mu.Unlock()
				rf.startElection()
				//time.Sleep(10 * time.Millisecond)
				return
			}
			rf.mu.Unlock()
		}
	}
}

//func (rf *Raft) sendAppendEntriesRetry(leaderTerm int, leaderId int, leaderCommit int, preLogIdx int, preLogTerm int, logs []*Logs, serverId int) bool {
//	appendEntriesArgs := &AppendEntriesArgs{
//		Term: leaderTerm, LeaderId: leaderId, LeaderCommit: leaderCommit,
//		PrevLogIdx: preLogIdx, PrevLogTerm: preLogTerm, Entries: logs,
//	}
//	appendEntriesReply := &AppendEntriesReply{}
//	rf.dlog("...leader retry send logs")
//	if ok := rf.sendAppendEntries(serverId, appendEntriesArgs, appendEntriesReply); !ok {
//		rf.dlog("...server %d timeout, retry sync entries", serverId)
//		return rf.sendAppendEntriesRetry(leaderTerm, leaderId, leaderCommit, preLogIdx, preLogTerm, logs, serverId)
//	}
//	rf.mu.Lock()
//	if !appendEntriesReply.Success {
//		//rf.nextIdx[serverId] -= 1
//		if appendEntriesReply.Term > rf.currentTerm {
//			rf.dlog("...leader find biger term: %v", appendEntriesReply.Term)
//			rf.becomeFollower(appendEntriesReply.Term)
//			rf.heartbeatTimer = time.Now()
//			rf.mu.Unlock()
//			return false
//		} else if appendEntriesReply.Term == rf.currentTerm {
//			if preLogIdx > 1 {
//				preLogIdx -= 1
//				preLogTerm = rf.Logs[preLogIdx-1].Term
//				logs = rf.Logs[preLogIdx:]
//			} else {
//				preLogIdx = 0
//				preLogTerm = 0
//				logs = rf.Logs
//			}
//			rf.mu.Unlock()
//			return rf.sendAppendEntriesRetry(leaderTerm, leaderId, leaderCommit, preLogIdx, preLogTerm, logs, serverId)
//		}
//	}
//	rf.dlog("...preLogIdx:%v, len(logs):%v", preLogIdx, len(logs))
//	rf.nextIdx[serverId] = preLogIdx + len(logs) + 1
//	rf.matchidx[serverId] = rf.nextIdx[serverId] - 1
//	rf.dlog("...leader sync log success to server %d nextIdx[i]:%v, matchIdx[i]:%v", serverId, rf.nextIdx[serverId], rf.matchidx[serverId])
//	rf.mu.Unlock()
//	return true
//}

func (rf *Raft) leaderSendHeart() {
	rf.mu.Lock()
	if rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	leaderTerm := rf.currentTerm
	leaderId := rf.me
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()
	rf.dlog("...leader prepare send heartbeat")
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		tmpI := i
		go func(tmpI int) {
			heartBeatArgs, heartBeatReply := &AppendEntriesArgs{Term: leaderTerm, LeaderId: leaderId, LeaderCommit: leaderCommit, Entries: nil}, &AppendEntriesReply{}
			rf.dlog("...leader send heartbeat")
			if ok := rf.sendHeartBeat(tmpI, heartBeatArgs, heartBeatReply); !ok {
				return
			}
			rf.dlog("...leader send heart success to %d", tmpI)
		}(tmpI)
	}
	return
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.dlog("...become Leader, curterm %d, logs:%v", rf.currentTerm, rf.Logs)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIdx[i] = len(rf.Logs) + 1
		rf.matchidx[i] = 0
	}
	//go func() {
	//	for {
	//		if rf.killed() {
	//			return
	//		}
	//		go rf.leaderSendHeart()
	//		select {
	//		case <-time.After(100 * time.Millisecond):
	//			rf.mu.Lock()
	//			if rf.state != Leader {
	//				rf.mu.Unlock()
	//				return
	//			}
	//			rf.mu.Unlock()
	//		}
	//	}
	//}()

	go func() {
		for {
			if rf.killed() {
				return
			}
			go rf.syncEntriesTicker()
			select {
			case <-time.After(100 * time.Millisecond):
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		}
	}()
}

func (rf *Raft) syncEntriesTicker() {
	rf.mu.Lock()
	peersNum := len(rf.peers)
	rf.dlog("...leader try to sync entries term %v index %v", rf.currentTerm, rf.commitIndex)
	if rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	//var wg sync.WaitGroup
	for i := 0; i < peersNum; i++ {
		if i == rf.me {
			continue
		}
		tmpI := i
		//wg.Add(1)
		go func(i int) {
			//defer wg.Done()
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			//rf.dlog("...leader lastLogIdx:%d, server-%d nextIdx:%d", lastLogIdx, i, rf.nextIdx[i])
			nextIdx := rf.nextIdx[i]
			if nextIdx <= 0 {
				rf.mu.Unlock()
				return
			}
			preLogIdx := nextIdx - 1
			preLogTerm := 0
			//rf.dlog("...preLogIdx:%d, preLogTerm:%v", preLogIdx, preLogTerm)
			if preLogIdx > 0 {
				preLogTerm = rf.Logs[preLogIdx-1].Term
			}
			logs := rf.Logs[nextIdx-1:]
			leaderTerm := rf.currentTerm
			leaderId := rf.me
			leaderCommit := rf.commitIndex
			rf.dlog("...leader %d sync and heart to server %d leaderTerm:%v, leaderCommit:%v nextIdx:%v, preLogIdx:%v, preLogTerm:%v, logs:%v",
				rf.me, i, leaderTerm, leaderCommit, nextIdx, preLogIdx, preLogTerm, logs)
			rf.mu.Unlock()

			appendEntriesArgs := &AppendEntriesArgs{
				Term: leaderTerm, LeaderId: leaderId, LeaderCommit: leaderCommit,
				PrevLogIdx: preLogIdx, PrevLogTerm: preLogTerm, Entries: logs,
			}
			appendEntriesReply := &AppendEntriesReply{}
			//rf.dlog("...leader send sync log proposal to %d", i)
			if ok := rf.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply); !ok {
				time.Sleep(10 * time.Millisecond)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if appendEntriesReply.Term > rf.currentTerm {
				rf.becomeFollower(appendEntriesReply.Term)
				rf.heartbeatTimer = time.Now()
				return
			}
			if rf.state != Leader {
				return
			}
			if appendEntriesReply.Term == rf.currentTerm {
				if appendEntriesReply.Success {
					rf.dlog("...recieve server %d sync log successfully", i)
					rf.dlog("...preLogIdx:%v, len(Logs):%v", preLogIdx, len(logs))
					rf.nextIdx[i] = preLogIdx + len(logs) + 1
					rf.matchidx[i] = rf.nextIdx[i] - 1
					curCommit := rf.commitIndex
					for idx := rf.commitIndex + 1; idx <= rf.matchidx[i]; idx++ {
						if rf.currentTerm == rf.Logs[idx-1].Term {
							matchNum := 1
							for p := 0; p < len(rf.peers); p++ {
								if p == rf.me {
									continue
								}
								rf.dlog("...server %d matchIdx: %d idx %d, leaderCurretTerm %d logs[idx-1].Term %d", p, rf.matchidx[p], idx, rf.currentTerm, rf.Logs[idx-1].Term)
								if rf.matchidx[p] >= idx {
									matchNum += 1
								}
							}
							rf.dlog("...matchNum:%v", matchNum)
							if matchNum*2 >= len(rf.peers)+1 {
								rf.dlog("...leader find most servers agree this log, index:%d term:%d", idx, rf.currentTerm)
								rf.commitIndex = idx
							}
						}
					}
					// 一次性 发送信号，不然信号还没同步完，其他节点就开始选举超时。
					if curCommit != rf.commitIndex {
						rf.dlog("...leader send sync log signal curCommit %d rf.commitIdx %d", curCommit, rf.commitIndex)
						if rf.killed() {
							return
						}
						rf.commitedSigal <- struct{}{}
					}
				} else {
					if appendEntriesReply.FirstConfiltIdx != -1 {
						rf.nextIdx[i] = appendEntriesReply.FirstConfiltIdx
					} else {
						rf.nextIdx[i] -= 1
					}
					rf.dlog("...server %d term or index not ok", i)
				}
			}
		}(tmpI)
		//time.Sleep(10 * time.Millisecond)
	}
	//wg.Wait()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidator
	rf.currentTerm += 1
	rf.voteFor = rf.me
	// 开启一轮选举后，直接重置选举超时时间，不然出现多个线程同时请求，导致结果很慢，可能存在活锁
	rf.heartbeatTimer = time.Now()
	go rf.persist()
	curTerm := rf.currentTerm
	candidateId := rf.me
	lastLogIdx := len(rf.Logs)
	accievedVotes := 1
	var lastLogTerm int
	if len(rf.Logs) == 0 {
		lastLogTerm = 0
	} else {
		lastLogTerm = rf.Logs[len(rf.Logs)-1].Term
	}
	//isSuccess := false

	//rf.dlog("...len peers: %d", len(rf.peers))
	rf.mu.Unlock()

	rf.dlog("...start election curTerm:%d", curTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.dlog("...request for i:%d to get vote", i)
		tmpI := i
		go func(tmpI int) {
			rf.mu.Lock()
			if rf.state != Candidator {
				rf.dlog("...rf.state:%d", rf.state)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			requestVoteArgs, requestVotereply := &RequestVoteArgs{curTerm, candidateId, lastLogIdx, lastLogTerm}, &RequestVoteReply{}
			if ok := rf.sendRequestVote(tmpI, requestVoteArgs, requestVotereply); !ok {
				rf.dlog("...send requestVote server %d not response", tmpI)
				time.Sleep(10 * time.Millisecond)
				return
			}
			defer rf.mu.Unlock()
			rf.mu.Lock()
			if rf.state != Candidator {
				rf.dlog("...rf.state:%d", rf.state)
				return
			}
			if requestVotereply.VoteGranted {
				accievedVotes += 1
				//rf.dlog("...get votes:%d", accievedVotes)
				rf.dlog("...accivecedVote+1, now is %d", accievedVotes)
				if accievedVotes*2 >= len(rf.peers)+1 {
					rf.becomeLeader()
					return
				}
			}
		}(tmpI)
	}
	//rf.mu.Unlock()
	// 以上操作全失败
	go rf.becomeCandidate()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	if rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//rf.mu.Lock()
		//if rf.voteFor == rf.me {
		//	rf.becomeLeader()
		//	rf.mu.Unlock()
		//} else if rf.voteFor != rf.me {
		//	rf.mu.Unlock()
		//	rf.becomeCandidate()
		//}
		//if state == Follower {
		go rf.becomeCandidate()
		//}
	}
}
func (rf *Raft) reportCommitedChan() {
	for range rf.commitedSigal {
		rf.mu.Lock()
		rf.dlog("...server %d prepare commit log", rf.me)
		//rf.lastApplied = rf.commitIndex

		var applyMsgs []ApplyMsg
		for i := rf.lastApplied; i < rf.commitIndex; i++ {
			applyMsg := ApplyMsg{Command: rf.Logs[i].Command, CommandValid: true, CommandIndex: i + 1}
			applyMsgs = append(applyMsgs, applyMsg)
		}
		rf.dlog("...lastApplied:%d, commitIdx:%d", rf.lastApplied, rf.commitIndex)
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		for _, msg := range applyMsgs {
			rf.dlog("...server %d commit log %v", rf.me, msg)
			rf.applyC <- msg
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = Follower
	rf.heartbeatTimer = time.Now()
	rf.voteFor = -1
	rf.Logs = make([]*Logs, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIdx, rf.matchidx = make([]int, len(rf.peers)), make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchidx[i] = 0
		rf.nextIdx[i] = 1
	}
	rf.applyC = applyCh
	rf.commitedSigal = make(chan struct{}, 16)
	rf.readPersist(persister.ReadRaftState())
	rf.dlog("...init or restart: %+v", rf)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.reportCommitedChan()
	return rf
}
