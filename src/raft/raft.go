package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type StateType uint

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (entry LogEntry) String() string {
	return fmt.Sprintf("<%v @%v T%v>", entry.Command, entry.Index, entry.Term)
}

const (
	Follower = iota
	Candidate
	Leader
)

var stateName = map[StateType]string{
	Follower:  "Follower",
	Candidate: "Candidate",
	Leader:    "Leader",
}

const (
	HeartbeatInterval  = 50 * time.Millisecond
	MinElectionTimeout = 150 * time.Millisecond
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	state           StateType // Follower, Candidate or Leader
	currentTerm     int
	votedFor        int // -1 for none
	electionTimeOut time.Duration
	electionTimer   *time.Timer // Follower becomes Candidate if timeout
	heartbeatTimer  *time.Timer // Leader sends heartbeat to Follower

	// 2B
	log         []LogEntry // starts from index 1
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
}

//
// This function need to be protected by mutex lock
//
func (rf *Raft) String() string {
	return fmt.Sprintf("[%v](%v)_T%v", stateName[rf.state], rf.me, rf.currentTerm)
}

func (rf *Raft) lock(position string) {
	rf.mu.Lock()
	if Debug > 1 {
		DPrintf("%v gets locked in %v", rf, position)
	}
}

func (rf *Raft) unlock(position string) {
	if Debug > 1 {
		DPrintf("%v is unlocked in %v", rf, position)
	}
	rf.mu.Unlock()
}

//
// Convert state to Follower
// This function need to be protected by mutex lock
//
func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) resetElectionTimer() {
	rf.heartbeatTimer.Stop()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.electionTimeOut)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A
	Term        int
	CandidateId int

	// 2B
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("Raft.RequestVote()")
	defer rf.unlock("Raft.RequestVote()")

	// invalid candidate
	if args.Term < rf.currentTerm ||
		// has already voted for another candidate
		args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("%v refuses voting request from (%v) for already voting for another candidate", rf, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.Term = args.Term

	// vote for this candidate
	if args.Term > rf.currentTerm {
		if rf.state != Follower {
			DPrintf("%v goes back to [%v]", rf, stateName[Follower])
		}

		if rf.state == Leader {
			rf.resetElectionTimer()
		}
		rf.convertToFollower(args.Term)
	}

	// check if candidate's log is at least as up-to-date as its
	followerLastIndex := rf.log[rf.lastLogIndex()].Term
	if args.LastLogTerm < followerLastIndex || args.LastLogTerm == followerLastIndex && args.LastLogIndex < rf.lastLogIndex() {
		DPrintf("%v refuses voting request from (%v) for already voting for out of date", rf, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	DPrintf("%v agrees with voting request from (%v)", rf, args.CandidateId)
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
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

//
// AppendEntries RPC arguments structure
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// 2A
	Term     int
	LeaderId int

	// 2B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC reply structure
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("Raft.AppendEntries()")
	defer rf.unlock("Raft.AppendEntries()")

	if args.Term < rf.currentTerm {
		DPrintf("%v refuses appending entries request from (%v) for out of date", rf, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Term = args.Term

	if rf.state != Follower {
		DPrintf("%v goes back to [%v]", rf, stateName[Follower])
	}
	rf.convertToFollower(args.Term)
	rf.resetElectionTimer()

	conflict := !(rf.lastLogIndex() >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm)
	// check if log is consistent with AppendEntries
	// compare index but not length of log, since follower may have more logs
	//if len(rf.log) < args.PrevLogIndex+1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	if conflict {
		DPrintf("%v refuses appending entries request from (%v) for conflict", rf, args.LeaderId)
		reply.Success = false
		return
	}

	// update local log
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)

	DPrintf("%v agrees with appending entries request from (%v)", rf, args.LeaderId)

	// apply commands to its state machine
	if args.LeaderCommit > rf.commitIndex {
		commitIndex := min(args.LeaderCommit, rf.lastLogIndex())
		rf.applyEntries(commitIndex)
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) isMajority(num int) bool {
	return num > len(rf.peers)/2
}

func (rf *Raft) routine() {
	for {
		select {
		case <-rf.electionTimer.C:
			{
				rf.lock("Raft.routine()")
				if rf.state == Follower {
					DPrintf("%v becomes a [%v]", rf, stateName[Candidate])
					rf.state = Candidate
				}
				rf.launchElection()
				rf.unlock("Raft.routine()")
			}
		case <-rf.heartbeatTimer.C:
			{
				rf.lock("Raft.routine()")
				if rf.state == Leader {
					rf.heartbeat()
					rf.heartbeatTimer.Reset(HeartbeatInterval)
				}
				rf.unlock("Raft.routine()")
			}
		}
	}
}

//
// Launch a new election by candidate
// This function need to be protected by mutex lock
//
func (rf *Raft) launchElection() {
	DPrintf("%v launches an election", rf)

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionTimer.Reset(rf.electionTimeOut)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.log[rf.lastLogIndex()].Term,
	}
	sendTerm := rf.currentTerm

	rf.unlock("Raft.launchElection()")

	cnt := 1
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		//
		// Candidate handles a vote election reply
		//
		handler := func(server int, reply *RequestVoteReply) {
			rf.lock("Raft.launchElection()RequestVoteReplyHandler()")
			defer rf.unlock("Raft.launchElection().RequestVoteReplyHandler()")

			// if message has been delayed, discard this replay
			if rf.currentTerm > sendTerm {
				return
			}

			// better leader exists, stop electing and go back to Follower
			if !reply.VoteGranted && reply.Term > rf.currentTerm {
				DPrintf("%v stops electing and goes back to [%v]", rf, stateName[Follower])
				rf.convertToFollower(reply.Term)
				rf.resetElectionTimer()
			}

			if rf.state == Candidate && reply.VoteGranted {
				DPrintf("%v gets a vote from (%v)", rf, server)
				cnt++
				if rf.isMajority(cnt) {
					DPrintf("%v is elected as [%v]", rf, stateName[Leader])
					rf.state = Leader
					rf.electionTimer.Stop()
					rf.heartbeat()
					rf.heartbeatTimer.Reset(HeartbeatInterval)
				}
			}
		}

		reply := &RequestVoteReply{}

		go func(server int) {
			if rf.sendRequestVote(server, args, reply) {
				handler(server, reply)
			}
		}(server)
	}

	rf.lock("Raft.launchElection()")
}

//
// Leader sends heartbeat to all followers
// This function need to be protected by mutex lock
//
func (rf *Raft) heartbeat() {
	DPrintf("%v starts broadcasting heartbeat", rf)

	sendTerm := rf.currentTerm

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		// send every entry not being replicated
		nextIndex := rf.nextIndex[server]
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		entries := make([]LogEntry, len(rf.log)-nextIndex)
		copy(entries, rf.log[nextIndex:])
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		reply := &AppendEntriesReply{}

		//
		// Leader handles a AppendEntries reply
		//
		handler := func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			rf.lock("Raft.heartbeat().AppendEntriesReplyHandler()")
			defer rf.unlock("Raft.heartbeat().AppendEntriesReplyHandler()")

			// if message has been delayed, discard this replay
			if rf.currentTerm > sendTerm {
				return
			}

			// better leader has been elected, go back to Follower
			if reply.Term > rf.currentTerm {
				DPrintf("%v knows a better leader has been elected and goes back to [%v]", rf, stateName[Follower])
				rf.convertToFollower(reply.Term)
				rf.resetElectionTimer()
				return
			}

			if reply.Success {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1

				DPrintf("%v gets a success AppendEntries reply from (%v) and sets its nextIndex to %v", rf, server, rf.nextIndex[server])

				// update commitIndex, n^2 solution
				for index := rf.lastLogIndex(); index > rf.commitIndex; index-- {
					// Figure 8
					if rf.log[index].Term != rf.currentTerm {
						break
					}

					cnt := 0
					for _, matched := range rf.matchIndex {
						if matched >= index {
							cnt++
						}
					}

					if rf.isMajority(cnt) {
						rf.applyEntries(index)
					}
				}
			} else {
				// TODO: optimized entries update
				rf.nextIndex[server] = max(rf.nextIndex[server]-1, rf.matchIndex[server]+1)
				DPrintf("%v gets a fail AppendEntries reply from (%v) and sets its nextIndex to %v", rf, server, rf.nextIndex[server])
			}
		}

		rf.unlock("Raft.heartbeat()")
		go func(server int) {
			if rf.sendAppendEntries(server, args, reply) {
				handler(server, args, reply)
			}
		}(server)
		rf.lock("Raft.heartbeat()")
	}
}

//
// apply commands to its state machine
// This function need to be protected by mutex lock
//
func (rf *Raft) applyEntries(commitIndex int) {
	rf.commitIndex = commitIndex
	if commitIndex > rf.lastApplied {
		DPrintf("%v tries to apply entries up to %v", rf, commitIndex)
		entries := rf.log[rf.lastApplied+1 : rf.commitIndex+1]
		rf.lastApplied = commitIndex
		go func() {
			for _, entry := range entries {
				msg := ApplyMsg{
					Index:   entry.Index,
					Command: entry.Command,
				}

				rf.applyCh <- msg
			}
		}()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).

	rf.lock("Raft.GetState()")
	defer rf.unlock("Raft.GetState()")

	term = rf.currentTerm
	isLeader = rf.state == Leader

	return term, isLeader
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.lock("Raft.Start()")
	defer rf.unlock("Raft.Start()")
	isLeader = rf.state == Leader
	term = rf.currentTerm

	if isLeader {
		index = len(rf.log)
		entry := LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		}
		DPrintf("%v receives a new command %v and logs it as %v", rf, command, entry)
		rf.log = append(rf.log, entry)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		// broadcast a heartbeat at once
		rf.heartbeat()
	} else {
		DPrintf("%v receives a new command %v but discards it for not being a Leader", rf, command)
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	// 2A
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimeOut = time.Duration((1.0 + rand.Float64()) * float64(MinElectionTimeout))
	rf.electionTimer = time.NewTimer(rf.electionTimeOut)
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)

	// 2B
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	nextIndex := len(rf.log)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = nextIndex
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.routine()

	return rf
}
