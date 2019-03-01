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
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	Snapshot []byte
}

//自定义的log(包含term和内容)

type LogOfRaft struct {
	Index   int
	Term    int
	Command interface{}
}

type StateOfRaft int32

const (
	Follower StateOfRaft = iota
	Candidate
	Leader
)

const VOTENULL int = -1
const HeartbeatInteval int = 50

func (s StateOfRaft) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
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

	currentTerm int //所能看到的最新的term
	voteFor     int
	logOfRaft   []LogOfRaft

	commitIndex int //最后一个commit的log index
	lastApplied int //apply到state machine的log index

	nextIndex  []int //对于leader来说，下一个发往其他server 的log entry 的index
	matchIndex []int //对于leader来说，被复制到其他server 的log entry 中 最大的index 值

	state StateOfRaft

	heartbeatInterval time.Duration

	appendEntryCh chan bool
	grantVoteCh   chan bool
	leaderCh      chan bool
	applyCh       chan ApplyMsg
	exitCh        chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) getIndexLogTerm(idx int) int {
	offset := idx - rf.logOfRaft[0].Index
	return rf.logOfRaft[offset].Term
}

func (rf *Raft) getIndexThroughPos(idx int) int { //将 logOfRaft中的位置转换为index。
	return idx + rf.logOfRaft[0].Index
}

func (rf *Raft) getPosThroughIndex(idx int) int { //将 logOfRaft中的位置转换为index。
	return idx - rf.logOfRaft[0].Index
}

func (rf *Raft) getLastLogIndex() int {

	return len(rf.logOfRaft) - 1 + rf.logOfRaft[0].Index
}

func (rf *Raft) getLastLogTerm() int {
	_lastLogIndex := len(rf.logOfRaft) - 1

	return rf.logOfRaft[_lastLogIndex].Term
}

func (rf *Raft) getPrevLogIndex(idx int) int { //idx为具体哪一台server的nextIndex
	return rf.nextIndex[idx] - 1
}

func (rf *Raft) getPrevLogTerm(idx int) int {
	prevLogIndex := rf.getPrevLogIndex(idx)
	return rf.getIndexLogTerm(prevLogIndex)

}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

func GetRandomElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(200)+300) * time.Millisecond
}

func (rf *Raft) convertToFollower(term int) {
	defer rf.persist()
	DPrintf(" Convert server(%v) state(%v=>follower) term(%v => %v)", rf.me,
		rf.state.String(), rf.currentTerm, term)

	log.Printf(" Convert server(%v) state(%v=>follower) term(%v => %v)", rf.me,
		rf.state.String(), rf.currentTerm, term)

	rf.state = Follower
	rf.currentTerm = term
	rf.voteFor = VOTENULL
}

func (rf *Raft) convertToCandidate() {
	defer rf.persist()
	DPrintf(" Convert server(%v) state(%v=>candidate) term(%v => %v)", rf.me,
		rf.state.String(), rf.currentTerm, rf.currentTerm+1)

	log.Printf(" Convert server(%v) state(%v=>candidate) term(%v => %v)", rf.me,
		rf.state.String(), rf.currentTerm, rf.currentTerm+1)
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
}

func (rf *Raft) convertToLeader() {
	defer rf.persist()
	if rf.state != Candidate {
		return
	}

	DPrintf(" Convert server(%v) state(%v=>leader) term %v", rf.me,
		rf.state.String(), rf.currentTerm)

	log.Printf(" Convert server(%v) state(%v=>leader) term %v", rf.me,
		rf.state.String(), rf.currentTerm)

	rf.state = Leader

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
}

func (rf *Raft) checkState(state StateOfRaft, term int) bool {
	return rf.state == state && rf.currentTerm == term
}

func dropAndSet(ch chan bool) {
	select {
	case <-ch:
	default:
	}

	ch <- true
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	DPrintf(" server %v persist success and current term is %v", rf.me, rf.currentTerm)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		log.Fatal(err)
	}
	err = e.Encode(rf.voteFor)
	if err != nil {
		log.Fatal(err)
	}
	err = e.Encode(rf.logOfRaft)
	if err != nil {
		log.Fatal(err)
	}
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logOfRaft []LogOfRaft
	err := d.Decode(&currentTerm)
	if err != nil {
		log.Fatal(err)
	}
	err = d.Decode(&voteFor)
	if err != nil {
		log.Fatal(err)
	}
	err = d.Decode(&logOfRaft)
	if err != nil {
		log.Fatal(err)
	}

	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logOfRaft = logOfRaft

	DPrintf(" server %v readPersist success and current term is %v", rf.me, rf.currentTerm)

}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
	//Done          bool

}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf(" rf %v was installsnapshot by rf %v", rf.me, args.LeaderId)
	rf.mu.Lock()
	//defer rf.mu.Unlock()

	DPrintf(" verify lock rf %v was installsnapshot by rf %v", rf.me, args.LeaderId)
	reply.Term = rf.currentTerm

	if rf.currentTerm < args.Term {
		rf.convertToFollower(args.Term)
	}
	if rf.currentTerm == args.Term {
		rf.state = Follower
		dropAndSet(rf.appendEntryCh)
		rf.WriteStateAndSnapShot(args.Data, args.LastIncludeIndex, args.LastIncludeTerm)
		rf.mu.Unlock()
		msg := ApplyMsg{CommandValid: false, Snapshot: args.Data}
		rf.applyCh <- msg
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	//filename := strconv.Itoa(args.CandidateId) + ".log"
	//file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//log.SetOutput(file)
	//log.Printf("%v go into sendRequestVote to %v", args.CandidateId, rf.me)
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func (rf *Raft) WriteStateAndSnapShot(snapshotData []byte, index int, term int) {
	log.Printf(" rf %v getLastLogIndex() is %v\t, index is %v", rf.me, rf.getLastLogIndex(), index)
	if rf.getLastLogIndex() < index {
		//该循环只有 installSnapshot 调用才会进入
		rf.logOfRaft = rf.logOfRaft[0:0]
		rf.logOfRaft = append(rf.logOfRaft, LogOfRaft{Index: index, Term: term})
		if term == -1 {
			log.Fatal(" term can't be -1 ")
		}
	} else {
		rf.logOfRaft = rf.logOfRaft[rf.getPosThroughIndex(index):]
	}
	log.Printf("after the snapshot,rf %v's log is %v", rf.me, rf.logOfRaft)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		log.Fatal(err)
	}
	err = e.Encode(rf.voteFor)
	if err != nil {
		log.Fatal(err)
	}
	err = e.Encode(rf.logOfRaft)
	if err != nil {
		log.Fatal(err)
	}
	stateData := w.Bytes()
	rf.persister.SaveStateAndSnapshot(stateData, snapshotData)
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).

	DPrintf(" before %v 's request,%v 's votefor is %v", args.CandidateId, rf.me, rf.voteFor)
	log.Printf(" before %v 's request,%v 's votefor is %v", args.CandidateId, rf.me, rf.voteFor)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf(" verify lock before %v 's request,%v 's votefor is %v", args.CandidateId, rf.me, rf.voteFor)
	log.Printf(" verify lock before %v 's request,%v 's votefor is %v", args.CandidateId, rf.me, rf.voteFor)

	DPrintf(" %v's requesetvote args is %v, and the reciever %v currentTerm is %v", args.CandidateId, *args, rf.me, rf.currentTerm)
	log.Printf(" %v's requesetvote args is %v, and the reciever %v currentTerm is %v", args.CandidateId, *args, rf.me, rf.currentTerm)

	// all servers
	if rf.currentTerm < args.Term {
		rf.convertToFollower(args.Term)
	}

	_voteGranted := false
	if rf.currentTerm == args.Term && (rf.voteFor == VOTENULL || rf.voteFor == args.CandidateId) && (rf.getLastLogTerm() < args.LastLogTerm || (rf.getLastLogTerm() == args.LastLogTerm && rf.getLastLogIndex() <= args.LastLogIndex)) {
		rf.state = Follower
		dropAndSet(rf.grantVoteCh)
		_voteGranted = true
		rf.voteFor = args.CandidateId
	}

	reply.VoteGranted = _voteGranted
	reply.Term = rf.currentTerm

	DPrintf(" after %v 's request,%v 's votefor is %v", args.CandidateId, rf.me, rf.voteFor)
	log.Printf(" after %v 's request,%v 's votefor is %v", args.CandidateId, rf.me, rf.voteFor)
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

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogOfRaft //此处假设一次仅发送一个log
	//Entries      map[int]LogOfRaft
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	success := false
	conflictIndex := rf.logOfRaft[0].Index //我希望得到的下一个log的index
	conflictTerm := rf.logOfRaft[0].Term

	DPrintf(" before recieve %v entry, %v's log is %v, the args is %v", args.LeaderId, rf.me, rf.logOfRaft, *args)
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if args.Term == rf.currentTerm {
		rf.state = Follower //这里如果不加会漏掉同一个 term 的 candidate，这样candidate 无法转换为 follower.
		dropAndSet(rf.appendEntryCh)
		if rf.getLastLogIndex() < args.PrevLogIndex {
			conflictIndex = rf.getLastLogIndex() + 1 //confilictIndex 是期望的下一个command的index, 之前的仍然有可能是全对的。
			conflictTerm = rf.logOfRaft[0].Term
		} else {
			prevLogTerm := rf.getIndexLogTerm(args.PrevLogIndex)
			if prevLogTerm != args.PrevLogTerm {
				conflictTerm = prevLogTerm
				for i := 1; i < len(rf.logOfRaft); i++ {
					if rf.logOfRaft[i].Term == conflictTerm {
						conflictIndex = rf.getIndexThroughPos(i)
						break
					}
				}
				//rf.logOfRaft = rf.logOfRaft[:args.PrevLogIndex]
				rf.logOfRaft = rf.logOfRaft[:rf.getPosThroughIndex(args.PrevLogIndex)]
			} else {
				success = true
				if args.Entries != nil {
					index := args.PrevLogIndex
					for i := 0; i < len(args.Entries); i++ {
						index += 1
						if index > rf.getLastLogIndex() {
							rf.logOfRaft = append(rf.logOfRaft, args.Entries[i:]...)
							break
						}
						//if rf.logOfRaft[index].Term != args.Entries[i].Term {
						//	DPrintf(" Term not equal, Server(%v=>%v), prevIndex=%v, index=%v", args.LeaderId, rf.me, args.PrevLogIndex, index)
						//	rf.logOfRaft = rf.logOfRaft[:index]
						//	rf.logOfRaft = append(rf.logOfRaft, args.Entries[i:]...)
						//	break
						//}
						if rf.getIndexLogTerm(index) != args.Entries[i].Term {
							DPrintf(" Term not equal, Server(%v=>%v), prevIndex=%v, index=%v", args.LeaderId, rf.me, args.PrevLogIndex, index)
							rf.logOfRaft = rf.logOfRaft[:rf.getPosThroughIndex(index)]
							rf.logOfRaft = append(rf.logOfRaft, args.Entries[i:]...)
							break
						}
					}
				}
				if rf.commitIndex < args.LeaderCommit {
					rf.commitIndex = intMin(args.LeaderCommit, rf.getLastLogIndex())
					rf.applyLogs()
				}
			}
		}

	}

	DPrintf(" after recieve %v entry, %v's log is %v\n", args.LeaderId, rf.me, rf.logOfRaft)

	reply.Term = rf.currentTerm
	reply.Success = success
	reply.ConflictIndex = conflictIndex
	reply.ConflictTerm = conflictTerm
	return

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//filename := strconv.Itoa(args.CandidateId) + ".log"
	//file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//log.SetOutput(file)
	//log.Printf("%v go into sendRequestVote to %v", args.CandidateId, rf.me)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
//// agreement and return immediately. there is no guarantee that this
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

	index := -1
	term := -1
	_, isLeader := rf.GetState()

	// Your code here (2B).

	if isLeader {
		rf.mu.Lock()
		//_commandIndex := len(rf.logOfRaft)
		_commandIndex := rf.getLastLogIndex() + 1
		rf.logOfRaft = append(rf.logOfRaft, LogOfRaft{_commandIndex, rf.currentTerm, command})
		//for i := 0; i < len(rf.peers); i++ {
		//	rf.nextIndex[i] = len(rf.logOfRaft)
		//}
		//index = len(rf.logOfRaft) - 1 //函数返回值
		index = _commandIndex
		term = rf.currentTerm //函数返回值
		rf.persist()
		DPrintf(" %v need to send command %v, and its index is %v", rf.me, command, index)
		rf.mu.Unlock()

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
	DPrintf(" Kill Server(%v)", rf.me)
	dropAndSet(rf.exitCh)
}

func (rf *Raft) applyLogs() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		if rf.lastApplied > rf.logOfRaft[0].Index {
			DPrintf(" Server(%v) applyLogs, commitIndex:%v, lastApplied:%v, command:%v", rf.me, rf.commitIndex, rf.lastApplied, rf.logOfRaft[rf.getPosThroughIndex(rf.lastApplied)].Command)
			//entry := rf.logOfRaft[rf.lastApplied]
			entry := rf.logOfRaft[rf.getPosThroughIndex(rf.lastApplied)]
			msg := ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: entry.Index}
			rf.applyCh <- msg
		}
		//DPrintf(" Server(%v) applyLogs, commitIndex:%v, lastApplied:%v, command:%v", rf.me, rf.commitIndex, rf.lastApplied, rf.logOfRaft[rf.getPosThroughIndex(rf.lastApplied)].Command)
		////entry := rf.logOfRaft[rf.lastApplied]
		//entry := rf.logOfRaft[rf.getPosThroughIndex(rf.lastApplied)]
		//msg := ApplyMsg{CommandValid : true, Command:entry.Command, CommandIndex:entry.Index}
		//rf.applyCh <- msg
	}
}

func (rf *Raft) advanceCommitIndex() {
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	//matchIndex[rf.me] = len(rf.logOfRaft) - 1
	matchIndex[rf.me] = rf.getLastLogIndex()
	sort.Ints(matchIndex)

	newCommitIndex := matchIndex[len(rf.peers)/2]
	//DPrintf(" in advance CommitIndex matchIndexes:%v, newCommitIndex:%v", matchIndex, newCommitIndex)

	//if rf.state == Leader && newCommitIndex > rf.commitIndex && rf.logOfRaft[newCommitIndex].Term == rf.currentTerm {
	if rf.state == Leader && newCommitIndex > rf.commitIndex && rf.logOfRaft[rf.getPosThroughIndex(newCommitIndex)].Term == rf.currentTerm {
		rf.commitIndex = newCommitIndex
		rf.applyLogs()
	}
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	args := &RequestVoteArgs{rf.currentTerm,
		rf.me, rf.getLastLogIndex(),
		rf.getLastLogTerm()}
	rf.mu.Unlock()

	var numVoted int32 = 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		DPrintf(" %v need to sendRequest to %v", rf.me, i)
		go func(idx int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ret := rf.sendRequestVote(idx, args, reply)
			DPrintf(" sendRequestVote(%v=>%v) args:%v %v", rf.me, idx, args, ret)

			if ret {

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm < reply.Term {
					rf.convertToFollower(reply.Term)
					DPrintf(" server %v state changes(%v=>%v)", rf.me, Candidate, Follower)
					return
				}

				if !rf.checkState(Candidate, args.Term) {
					return
				}

				if reply.VoteGranted {
					atomic.AddInt32(&numVoted, 1)
				}

				if atomic.LoadInt32(&numVoted) > int32(len(rf.peers)/2) {
					DPrintf(" Server(%d) win vote", rf.me)
					rf.convertToLeader()
					dropAndSet(rf.leaderCh)
				}
			}
		}(i, args)
	}

}

func (rf *Raft) startInstallSnapshots(idx int) bool {
	args := &InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.logOfRaft[0].Index,
		rf.logOfRaft[0].Term,
		rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapShot(idx, args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			return false
		} else {
			rf.matchIndex[idx] = args.LastIncludeIndex
			rf.nextIndex[idx] = rf.matchIndex[idx] + 1
			rf.advanceCommitIndex()
			return true
		}
	}
	return false
}

func (rf *Raft) startAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(idx int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				//nextIndex := rf.nextIndex[idx]

				if rf.nextIndex[idx] <= rf.logOfRaft[0].Index {
					log.Printf("come here")
					if !rf.startInstallSnapshots(idx) {
						rf.mu.Unlock()
						return
					}
				}

				nextIndex := rf.nextIndex[idx]

				entries := make([]LogOfRaft, 0)
				//entries = append(entries, rf.logOfRaft[nextIndex:]...)
				//log.Printf( " nextIndex[%v] is %v, and the pos is %v", idx, nextIndex, rf.getPosThroughIndex(nextIndex))
				entries = append(entries, rf.logOfRaft[rf.getPosThroughIndex(nextIndex):]...)
				args := &AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					rf.getPrevLogIndex(idx),
					rf.getPrevLogTerm(idx),
					entries,
					rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()

				ret := rf.sendAppendEntries(idx, args, reply)
				if !ret { //发送失败我就不发了， 等待下一个心跳继续发送
					return
				}

				DPrintf(" server %v send appendentries to %v and the reply is %v", rf.me, idx, reply)

				rf.mu.Lock()

				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					DPrintf(" server %v state changes(%v=>%v)", rf.me, Leader, Follower)
					rf.mu.Unlock()
					return
				}

				//if reply.ConflictIndex <= rf.logOfRaft[0].Index{
				//	rf.startInstallSnapshots(idx)
				//	rf.mu.Unlock()
				//	return
				//}

				if reply.Success {
					rf.matchIndex[idx] = args.PrevLogIndex + len(entries)
					rf.nextIndex[idx] = rf.matchIndex[idx] + 1
					DPrintf(" SendAppendEntries Success(%v => %v), nextIndex:%v, matchIndex:%v", rf.me, idx, rf.nextIndex, rf.matchIndex)
					rf.advanceCommitIndex()
					rf.mu.Unlock()
					return
				} else {
					newIndex := reply.ConflictIndex
					for i := 1; i < len(rf.logOfRaft); i++ {
						entry := rf.logOfRaft[i]
						if entry.Term == reply.ConflictTerm {
							//newIndex = i + 1
							newIndex = rf.getIndexThroughPos(i + 1)
							//如果接受者和发送者都有某个term的第一个entry，那么这个entry一定是一样的
							//并且如果term 是 0的话，刚好掠过了第一个
						}
					}
					if newIndex == 0 {
						log.Fatal("newIndex can't be 0")
					}
					rf.nextIndex[idx] = newIndex
					//if newIndex <= rf.logOfRaft[0].Index{
					//	rf.startInstallSnapshots(idx)
					//	rf.mu.Unlock()
					//	return
					//}else{
					//	rf.nextIndex[idx] = newIndex
					//}

					//rf.nextIndex[idx] = intMax(rf.logOfRaft[0].Index + 1, newIndex)

					DPrintf(" SendAppendEntries failed(%v => %v), decrease nextIndex(%v):%v", rf.me, idx, idx, rf.nextIndex)
					rf.mu.Unlock()
				}
			}
		}(i)
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
	rf.voteFor = VOTENULL
	rf.logOfRaft = make([]LogOfRaft, 0)
	rf.logOfRaft = append(rf.logOfRaft, LogOfRaft{0, 0, nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower

	rf.heartbeatInterval = time.Duration(HeartbeatInteval) * time.Millisecond

	rf.grantVoteCh = make(chan bool, 1)
	rf.appendEntryCh = make(chan bool, 1)
	rf.leaderCh = make(chan bool, 1)
	rf.exitCh = make(chan bool, 1)

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			DPrintf(" %v is alive and its state is %v", rf.me, rf.state)
			select {
			case <-rf.exitCh:
				DPrintf(" Exit Server(%v)", rf.me)
				break
			default:
			}
			electionTimeout := GetRandomElectionTimeout()
			rf.mu.Lock()
			state := rf.state
			//log.Printf(" Server(%d) state:%v, electionTimeout:%v", rf.me, state, electionTimeout)
			rf.mu.Unlock()

			switch state {
			case Follower:
				select {
				case <-rf.appendEntryCh:
				case <-rf.grantVoteCh:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case Candidate:
				go rf.leaderElection()
				select {
				case <-rf.appendEntryCh:
				case <-rf.grantVoteCh:
				case <-rf.leaderCh:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case Leader:
				rf.startAppendEntries()
				time.Sleep(rf.heartbeatInterval)
			}

		}
	}()

	return rf
}
