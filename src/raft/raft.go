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
	"labrpc"
	"log"
	"math/rand"
	"sync"
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

	voteFor      map[int]int
	currentTerm  int //所能看到的最新的term
	logTerm      []int
	lastLogTerm  int //最后一个log的term
	lastLogIndex int
	leader       chan int
	candidate    chan int
	follower     chan int
	timeout      *time.Timer
	elapse       time.Duration
	timing       chan int
	state        int // 0 表示 follower， 1 表示 candidate, 2表示leader
	unnatural    int // 0 表示正常死亡， 1表示非正常死亡，主要是为了区分当时钟进行判读的时候，如果是由C或
	// L转换而来，则继续follower,如果是单纯的follower计数完毕， 则转换为 candidate.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == 2 {
		isleader = true
	} else {
		isleader = false
	}
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
	rf.mu.Lock()
	log.Printf(" before %v 's request,%v 's logterm is %v", args.CandidateId, rf.me, rf.voteFor)
	log.Printf(" %v's requesetvote args is %v, and the reciever %v currentTerm is %v", args.CandidateId, *args, rf.me, rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.lastLogTerm < args.LastLogTerm || (rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex <= args.LastLogIndex) {
		if val, ok := rf.voteFor[args.Term]; (ok && (val == args.CandidateId)) || !ok {
			reply.VoteGranted = true
			rf.voteFor[args.Term] = args.CandidateId
		}
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.unnatural = 1
		rf.state = 0
		rf.timeout.Reset(0)
	}
	log.Printf(" after %v 's request,%v 's logterm is %v", args.CandidateId, rf.me, rf.voteFor)
	rf.mu.Unlock()
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
	//filename := strconv.Itoa(args.CandidateId) + ".log"
	//file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//log.SetOutput(file)
	//log.Printf("%v go into sendRequestVote to %v", args.CandidateId, rf.me)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	LastLogIndex int
	LastLogTerm  int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	log.Printf(" %v's heartbeat args is %v, and the reciever %v currentTerm is %v", args.LeaderId, *args, rf.me, rf.currentTerm)
	log.Printf(" before %v 's heartbeat,%v 's logterm is %v", args.LeaderId, rf.me, rf.voteFor)
	reply.Term = rf.currentTerm
	reply.Success = true
	if args.Entries == nil {
		if rf.currentTerm <= args.Term {
			log.Printf(" %v's heartbeat to reciever %v currentTerm is success", args.LeaderId, rf.me)
			rf.currentTerm = args.Term
			rf.state = 0
			rf.unnatural = 1
			log.Printf(" %v timer was reset in recive heartbeat \n", rf.me)
			rf.timeout.Reset(0)

		} else {
			log.Printf(" %v's heartbeat to reciever %v currentTerm is failure", args.LeaderId, rf.me)
			reply.Success = false
		}
	}
	log.Printf(" after %v 's heartbeat,%v 's logterm is %v", args.LeaderId, rf.me, rf.voteFor)
	rf.mu.Unlock()
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

	appendInterval := time.Duration(50 * time.Millisecond)

	rf.follower = make(chan int, 0)
	rf.candidate = make(chan int, 0)
	rf.leader = make(chan int, 0)
	rf.timing = make(chan int, 0)
	rf.voteFor = make(map[int]int)
	rf.currentTerm = 0
	rf.lastLogIndex = 0
	rf.lastLogTerm = 0
	rf.state = 0
	rf.unnatural = 0

	fn := func() {
		rf.timing <- 1
		log.Printf(" timer is trigger, %v 's state : %v, its unnatural : %v\n", rf.me, rf.state, rf.unnatural)
		switch rf.state {
		case 0:
			{
				rf.mu.Lock()
				if rf.unnatural == 0 {
					log.Printf(" %v state change from follower to candidate\n", rf.me)
					rf.state = 1
					rf.timeout.Reset(rf.elapse)
					log.Printf(" %v timer was reset in case0--if and the elapse is %v", rf.me, rf.elapse)
					rf.mu.Unlock()
					rf.candidate <- 1
				} else {
					log.Printf(" %v state keeps as follower\n", rf.me)
					rf.unnatural = 0
					rf.timeout.Reset(rf.elapse)
					log.Printf(" %v timer was reset in case0--else and the elapse is %v", rf.me, rf.elapse)
					rf.mu.Unlock()
					rf.follower <- 1
				}
			}
		case 1:
			{
				log.Printf(" %v state change from candidate to candidate\n", rf.me)
				rf.mu.Lock()
				rf.elapse = time.Duration(rand.Intn(200)+300) * time.Millisecond
				rf.timeout.Reset(rf.elapse)
				log.Printf(" %v timer was reset in case1 and the elapse is %v\n", rf.me, rf.elapse)
				rf.mu.Unlock()
				rf.candidate <- 1
			}
		case 2:
			{
				log.Printf(" %v state keeps as leader\n", rf.me)
				rf.mu.Lock()
				rf.timeout.Reset(appendInterval)
				rf.mu.Unlock()
				rf.leader <- 1
			}
		}
	}

	go func() {
		for {
			<-rf.follower
			log.Printf(" %v into follower stage and current term is %v", rf.me, rf.currentTerm)
			<-rf.timing
			log.Printf(" %v out of follower stage\n", rf.me)

		}
	}()

	go func() {
		for {
			<-rf.candidate
			rf.mu.Lock()
			rf.currentTerm += 1
			rf.voteFor[rf.currentTerm] = rf.me
			args := &RequestVoteArgs{rf.currentTerm, me, rf.lastLogIndex, rf.lastLogTerm}
			reply := &RequestVoteReply{}
			log.Printf(" %v into candidate stage and current term is %v", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			var voteToGet int
			if len(peers)%2 == 1 {
				voteToGet = (len(peers) + 1) / 2
			} else {
				voteToGet = len(peers)/2 + 1
			}
			log.Printf(" the vote to get is %v", voteToGet)
			//if voteToGet == 1 {
			//	log.Printf(" %v in candidate is continue", rf.me)
			//	continue
			//}
			sum := make([]int, len(peers))
			sum[rf.me] = 1
			for i := 0; i < len(peers); i++ {
				log.Printf(" %v start send request to %v", rf.me, i)
				if i != rf.me {
					i := i
					go func() {
						fterm := rf.currentTerm
						if rf.sendRequestVote(i, args, reply) {
							if reply.VoteGranted {
								sum[i] = 1
							} else {
								if reply.Term > fterm && fterm == rf.currentTerm {
									rf.mu.Lock()
									rf.state = 0
									rf.unnatural = 1
									rf.timeout.Reset(0)
									rf.mu.Unlock()
								}
							}
						}
					}()
				}
			}
			go func() {
				fterm := rf.currentTerm
				time.Sleep(rf.elapse - 10*time.Millisecond) // 这个10ms是考虑从计时器开始计时到启动这个goroutine所用的开销
				num := 0
				for _, value := range sum {
					num += value
				}
				if num >= voteToGet && fterm == rf.currentTerm && rf.state == 1 {
					log.Printf(" %v become leader", rf.me)
					rf.mu.Lock()
					rf.state = 2
					rf.timeout.Reset(0)
					rf.mu.Unlock()
				}
			}()
			<-rf.timing
			log.Printf(" %v out of candidate stage\n", rf.me)
		}
	}()

	go func() {
		for {
			<-rf.leader
			log.Printf(" %v into leader and current term is %v", rf.me, rf.currentTerm)
			rf.mu.Lock()
			args := &AppendEntriesArgs{rf.currentTerm, me, 0, 0, 0, 0, nil, 0}
			reply := &AppendEntriesReply{}
			fterm := rf.currentTerm
			rf.mu.Unlock()
			for i := 0; i < len(peers); i++ {
				i := i
				if i != rf.me {
					go func() {
						if rf.sendAppendEntries(i, args, reply) {
							log.Printf(" %v send heartbeat to %v and the reply is %v", rf.me, i, reply)
							if !reply.Success {
								if reply.Term > fterm && fterm == rf.currentTerm && rf.state == 2 {
									rf.mu.Lock()
									rf.state = 0
									rf.unnatural = 1
									rf.timeout.Reset(0)
									log.Printf(" %v from leader to follower\n", rf.me)
									rf.mu.Unlock()
								}
							}
						}
					}()
				}
			}
			<-rf.timing
			log.Printf(" %v out of leader stage\n", rf.me)
		}
	}()

	rf.follower <- 1
	log.Printf(" %v is initial\n", rf.me)
	rf.elapse = time.Duration(rand.Intn(200)+300) * time.Millisecond
	log.Printf(" %v 's elapse is :\t%v\n", rf.me, rf.elapse)
	rf.timeout = time.AfterFunc(rf.elapse, fn)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
