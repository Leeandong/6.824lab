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
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"
)
import "labrpc"

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

	term        int
	isLeader    bool
	voteFor     map[int]int
	currentTerm int
	logTerm     []int
	leader      chan int
	candidate   chan int
	follower    chan int
	timeout     *time.Timer
	elapse      time.Duration
	timing      chan int
	state       int // 0 表示 follower， 1 表示 candidate, 2表示leader

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}
	reply.VoteGranted = false
	if len(rf.logTerm)-1 < args.LastLogIndex || (len(rf.logTerm)-1 == args.LastLogIndex && rf.logTerm[args.LastLogIndex] <= args.LastLogTerm) {
		if val, ok := rf.voteFor[rf.currentTerm]; ok && val == args.CandidateId {
			reply.VoteGranted = true
			rf.voteFor[rf.currentTerm] = args.CandidateId
		}
	}

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

	rf.follower = make(chan int, 0)
	rf.candidate = make(chan int, 0)
	rf.leader = make(chan int, 0)
	rf.term = 0

	file, err := os.OpenFile("info.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	log.SetOutput(file)
	log.Print("Logging to a file in Go!")

	rf.state = 0
	rf.isLeader = false
	//f := func() {
	//	select{
	//	case <- rf.follower:{
	//		//rf.mu.Lock()
	//		//defer rf.mu.Unlock()
	//		rf.timeout.Reset(rf.elapse)
	//		rf.candidate <- 1
	//	}
	//	case <- rf.candidate:{
	//		//rf.mu.Lock()
	//		//defer rf.mu.Unlock()
	//		rf.elapse = time.Duration(rand.Intn(200)+300) * time.Millisecond
	//		rf.timeout.Reset(rf.elapse)
	//		rf.candidate <- 1
	//	}
	//	case <- rf.leader:{
	//		//rf.mu.Lock()
	//		//defer rf.mu.Unlock()
	//		rf.timeout.Reset(rf.elapse)
	//		rf.leader <- 1
	//	}
	//	}
	//	rf.timing <- 1
	//}

	fn := func() {
		rf.timing <- 1
		switch rf.state {
		case 0:
			{
				rf.timeout.Reset(rf.elapse)
				rf.candidate <- 1
			}
		case 1:
			{
				rf.elapse = time.Duration(rand.Intn(200)+300) * time.Millisecond
				rf.timeout.Reset(rf.elapse)
				rf.candidate <- 1
			}
		case 2:
			{
				rf.timeout.Reset(rf.elapse)
				rf.leader <- 1
			}
		}
	}

	rf.elapse = time.Duration(rand.Intn(200)+300) * time.Millisecond
	rf.timeout = time.AfterFunc(rf.elapse, fn)

	go func() {
		log.Print("waiting for the follower state")
		<-rf.follower
		rf.mu.Lock()
		_, file, line, ok := runtime.Caller(0)
		if ok {
			log.Printf("lock in file: %s    line=%d\n", file, line)
		}
		rf.state = 0
		rf.mu.Unlock()
		_, file, line, ok = runtime.Caller(0)
		if ok {
			log.Printf("unlock in file: %s    line=%d\n", file, line)
		}
		<-rf.timing
	}()

	go func() {
		<-rf.candidate
		rf.mu.Lock()
		_, file, line, ok := runtime.Caller(0)
		if ok {
			log.Printf("lock in file: %s    line=%d\n", file, line)
		}
		rf.state = 1
		rf.isLeader = false
		rf.mu.Unlock()
		_, file, line, ok = runtime.Caller(0)
		if ok {
			log.Printf("unlock in file: %s    line=%d\n", file, line)
		}
		args := &RequestVoteArgs{rf.currentTerm, me, len(rf.logTerm), rf.logTerm[len(rf.logTerm)-1]}
		reply := &RequestVoteReply{}
		sum := make([]int, len(peers))
		for i := 0; i < len(peers); i++ {
			i := i
			if i != me {
				go func() {
					rf.mu.Lock()
					_, file, line, ok := runtime.Caller(0)
					if ok {
						log.Printf("lock in file: %s    line=%d\n", file, line)
					}
					if sum[i] != 1 {
						if rf.sendRequestVote(i, args, reply) {
							sum[i] = 1
						} else {
							if reply.Term > rf.currentTerm {
								rf.follower <- 1
								rf.timeout.Reset(0)
							}
						}
					}
					rf.mu.Unlock()
					_, file, line, ok = runtime.Caller(0)
					if ok {
						log.Printf("unlock in file: %s    line=%d\n", file, line)
					}
				}()
			}
		}
		go func() {
			rf.mu.Lock()
			_, file, line, ok := runtime.Caller(0)
			if ok {
				log.Printf("lock in file: %s    line=%d\n", file, line)
			}
			num := 0
			for i := range sum {
				num += i
			}
			if num > len(peers)/2 {
				rf.timeout.Reset(0)
				rf.leader <- 1
			}
			rf.mu.Unlock()
			_, file, line, ok = runtime.Caller(0)
			if ok {
				log.Printf("unlock in file: %s    line=%d\n", file, line)
			}
		}()
		<-rf.timing
	}()

	go func() {
		<-rf.leader
		rf.mu.Lock()
		_, file, line, ok := runtime.Caller(0)
		if ok {
			log.Printf("lock in file: %s    line=%d\n", file, line)
		}
		rf.isLeader = true
		rf.state = 2
		rf.mu.Unlock()
		_, file, line, ok = runtime.Caller(0)
		if ok {
			log.Printf("unlock in file: %s    line=%d\n", file, line)
		}
		<-rf.timing
	}()

	log.Print("rf.follower is waiting")
	rf.follower <- 1
	time.Sleep(5 * time.Second)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
