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

//自定义的log(包含term和内容)

type LogOfRaft struct {
	Term    int
	Command interface{}
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

	voteFor     map[int]int
	currentTerm int //所能看到的最新的term
	//logTerm      map[int]int
	//lastLogIndex int
	leader    chan int
	candidate chan int
	follower  chan int
	timeout   *time.Timer
	elapse    time.Duration
	timing    chan int
	state     int      // 0 表示 follower， 1 表示 candidate, 2表示leader
	unnatural chan int // 0 表示正常死亡， 1表示非正常死亡，主要是为了区分当时钟进行判读的时候，如果是由C或
	// L转换而来，则继续follower,如果是单纯的follower计数完毕， 则转换为 candidate.
	//在用作刚刚被选举为leader， 将unnatural设置为1， 此时nextIndex 以及 matchIndex需要重置，

	// partB 补充的状态
	commitIndex int //最后一个commit的log index
	lastApplied int //apply到state machine的log index
	//log         map[int]interface{}  //log的内容

	logOfRaft map[int]LogOfRaft

	nextIndex     []int //对于leader来说，下一个发往其他server 的log entry 的index
	matchIndex    []int //对于leader来说，被复制到其他server 的log entry 中 最大的index 值
	commitChannel chan int
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
	log.Printf(" before %v 's request,%v 's votefor is %v", args.CandidateId, rf.me, rf.voteFor)
	log.Printf(" %v's requesetvote args is %v, and the reciever %v currentTerm is %v", args.CandidateId, *args, rf.me, rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term
		if rf.logOfRaft[len(rf.logOfRaft)-1].Term < args.LastLogTerm || (rf.logOfRaft[len(rf.logOfRaft)-1].Term == args.LastLogTerm && len(rf.logOfRaft)-1 <= args.LastLogIndex) {
			if val, ok := rf.voteFor[args.Term]; (ok && (val == args.CandidateId)) || !ok {
				reply.VoteGranted = true
				rf.voteFor[args.Term] = args.CandidateId
			}
		}
	}
	if reply.VoteGranted == true {
		log.Printf(" %v 's rf.unnatural <- 1 in receive RequestVote", rf.me)
		rf.unnatural <- 1
		rf.state = 0
		rf.timeout.Reset(0)
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
	}

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
	Entries      *LogOfRaft //此处假设一次仅发送一个log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.Success = false
	//即使只是传一个普通的心跳，也要做连续性检查，如果回复正确，等于告诉leader我们的log 包含prevLogIndex并且和leader相匹配
	//这会导致leader认为entry已经被提交到大部分的机器上，从而去commit.
	if args.Entries == nil {
		log.Printf(" %v receive a heartbeat and the params is %v, before receive, it's term is %v, log is %v, log length is %v, commitIndex is %v", rf.me, args, rf.currentTerm, rf.logOfRaft, len(rf.logOfRaft)-1, rf.commitIndex)
	} else {
		log.Printf(" %v receive a log and the params is %v, before receive, it's term is %v, log is %v, log length is %v, commitIndex is %v", rf.me, args, rf.currentTerm, rf.logOfRaft, len(rf.logOfRaft)-1, rf.commitIndex)

	}

	if rf.currentTerm <= args.Term {
		if rf.state != 2 {
			val, ok := rf.logOfRaft[args.PrevLogIndex]
			if !ok {
				reply.Success = false
			} else if ok && val.Term != args.PrevLogTerm {
				reply.Success = false
				for k := range rf.logOfRaft {
					if k >= args.PrevLogIndex {
						delete(rf.logOfRaft, k)
					}
				}
			} else {
				reply.Success = true
				if args.Entries != nil {
					rf.logOfRaft[args.PrevLogIndex+1] = *args.Entries
				}
				if args.LeaderCommit > rf.commitIndex { //关于commit应该只有在成功时才	可以commitIndex
					_oldCommitIndex := rf.commitIndex
					if len(rf.logOfRaft)-1 <= args.LeaderCommit {
						rf.commitIndex = len(rf.logOfRaft) - 1
					} else {
						rf.commitIndex = args.LeaderCommit
					}
					for i := _oldCommitIndex + 1; i <= rf.commitIndex; i++ {
						rf.commitChannel <- i
					}
				}
			}
		} else {
			log.Printf(" leader %v into follower", rf.me)
		}

		rf.unnatural <- 1

		rf.currentTerm = args.Term
		rf.state = 0
		if args.Entries == nil {
			log.Printf(" %v's timer is reset when receive heartbeat", rf.me)
		} else {
			log.Printf(" %v's timer is reset when receive log", rf.me)
		}
		rf.timeout.Reset(0)
		if args.Entries == nil {
			log.Printf(" after receive the %v's heartbeat, %v's term is %v, log is %v, log length is %v, commitIndex is %v", args.LeaderId, rf.me, rf.currentTerm, rf.logOfRaft, len(rf.logOfRaft)-1, rf.commitIndex)
		} else {
			log.Printf(" after receive the %v's log %v, %v's term is %v, log is %v, log length is %v, commitIndex is %v", args.LeaderId, args.Entries, rf.me, rf.currentTerm, rf.logOfRaft, len(rf.logOfRaft)-1, rf.commitIndex)
		}
		rf.mu.Unlock()
		//rf.unnatural <- 1

	} else {
		rf.mu.Unlock()
		reply.Success = false
	}

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

	quit := make(chan int)
	var _majority int
	if len(rf.peers)%2 == 1 {
		_majority = (len(rf.peers) + 1) / 2
	} else {
		_majority = len(rf.peers)/2 + 1
	}

	//如果是leader的话

	if isLeader {
		rf.mu.Lock()
		_commandEntry := LogOfRaft{rf.currentTerm, command} //当前所需要发送的命令
		_commandIndex := len(rf.logOfRaft)
		for k, v := range rf.logOfRaft { //_commandIndex 代表所需要提交的command的index
			if v == _commandEntry {
				_commandIndex = k
				break
			}
		}
		//fmt.Printf("%v is invoked to send command %v and commandindex is %v\n", rf.me, command, _commandIndex)
		//log.Printf(" %v is invoked to send command %v and commandindex is %v", rf.me, command, _commandIndex)
		if _commandIndex == len(rf.logOfRaft) {
			rf.logOfRaft[_commandIndex] = _commandEntry
		}

		_term := rf.currentTerm        //保存下当前所需要的
		_commitIndex := rf.commitIndex //当前所提交的index

		_nextIndex := rf.nextIndex
		_matchIndex := rf.matchIndex

		for k := range _nextIndex {
			_nextIndex[k] = _commandIndex
		}

		index = _commandIndex
		term = _term

		log.Printf(" %v need to send log to others and current log entry is %v, its index is %v", rf.me, rf.logOfRaft[_commandIndex], _commandIndex)
		rf.mu.Unlock()

		go func() {
			for {
				rf.mu.Lock()
				if _term == rf.currentTerm && rf.state == 2 && rf.commitIndex < _commandIndex {
					sum := 1
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me && _matchIndex[i] == _commandIndex {
							sum += 1
						}
					}
					log.Printf(" %v get %v agree on log %v", rf.me, sum, rf.logOfRaft[_commandIndex].Command)
					//rf.mu.Lock()
					//if sum >= _majority && rf.commitIndex < _commandIndex{
					if sum >= _majority {
						log.Printf(" %v commit log %v and old commitindex is %v, the new commit index is %v", rf.me, rf.logOfRaft[_commandIndex].Command, rf.commitIndex, _commandIndex)
						_oldCommitIndex := rf.commitIndex
						rf.commitIndex = _commandIndex
						for i := _oldCommitIndex + 1; i <= rf.commitIndex; i++ {
							rf.commitChannel <- i
						}
						rf.mu.Unlock()
						return
					} else {
						rf.mu.Unlock()
					}
				} else {
					rf.mu.Unlock()
					return
				}
				select {
				case <-quit:
					{
						return
					}
				default:
					{
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()

		for i := 0; i < len(rf.peers); i++ {
			i := i
			if i != rf.me {
				go func() {
					for {
						_currentSentLastIndex := _nextIndex[i] - 1
						if rf.currentTerm != _term {
							quit <- 1
							return
						}
						rf.mu.Lock()
						entryToSend := rf.logOfRaft[_currentSentLastIndex+1]
						log.Printf(" before %v send log entry %v to %v, its log is %v, its nextindex is %v", rf.me, entryToSend, i, rf.logOfRaft, _nextIndex)
						args := &AppendEntriesArgs{_term, rf.me, _currentSentLastIndex, rf.logOfRaft[_currentSentLastIndex].Term, &entryToSend, _commitIndex}
						reply := &AppendEntriesReply{}
						rf.mu.Unlock()
						if rf.sendAppendEntries(i, args, reply) {
							log.Printf(" %v send log entry to %v and the reply is %v", rf.me, i, reply)
							if _term == rf.currentTerm && rf.state == 2 {
								if !reply.Success {
									if reply.Term > _term {
										rf.mu.Lock()
										rf.unnatural <- 1
										rf.currentTerm = reply.Term
										rf.state = 0
										rf.timeout.Reset(0)
										log.Printf(" %v from leader to follower\n", rf.me)
										rf.mu.Unlock()
										quit <- 1
										return
									} else {
										if _currentSentLastIndex > 0 {
											_nextIndex[i] = _currentSentLastIndex
										}
									}
								} else {
									log.Printf(" %v has send log entry %v to %v", rf.me, entryToSend, i)
									_matchIndex[i] = _currentSentLastIndex + 1
									_nextIndex[i] = _currentSentLastIndex + 1 + 1
									if _nextIndex[i] == _commandIndex+1 {
										log.Printf(" %v send log entry to %v success", rf.me, i)
										rf.mu.Lock()
										for k := range _nextIndex {
											rf.nextIndex[k] = _nextIndex[k]
											rf.matchIndex[k] = _matchIndex[k]
										}
										rf.mu.Unlock()
										return
									}
								}
							} else {
								quit <- 1
								return
							}
						}
						time.Sleep(10 * time.Millisecond)
					}
				}()
			}
		}
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

	appendInterval := time.Duration(50 * time.Millisecond)

	rf.follower = make(chan int, 0)
	rf.candidate = make(chan int, 0)
	rf.leader = make(chan int, 0)
	rf.timing = make(chan int, 0)
	rf.voteFor = make(map[int]int)
	//rf.logTerm = make(map[int]int)
	rf.commitChannel = make(chan int, 0)
	rf.unnatural = make(chan int, 1)
	//rf.logTerm[0] = 0 //为了一致性检查的方便做的
	rf.logOfRaft = make(map[int]LogOfRaft)
	rf.logOfRaft[0] = LogOfRaft{0, nil}
	rf.currentTerm = 0
	rf.state = 0
	//rf.log  = make(map[int]interface{})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	fn := func() {
		log.Printf(" timer is trigger, %v 's state : %v, its unnatural : %v\n", rf.me, rf.state, rf.unnatural)
		rf.timing <- 1
		switch rf.state {
		case 0:
			{
				//time.Sleep(10 * time.Millisecond)
				select {
				case <-rf.unnatural:
					{ //非正常死亡
						log.Printf(" %v state keeps as follower\n", rf.me)
						rf.mu.Lock()
						rf.timeout.Reset(rf.elapse)
						log.Printf(" %v timer was reset unnatural and the elapse is %v", rf.me, rf.elapse)
						rf.mu.Unlock()
						rf.follower <- 1
					}
				default:
					{ //正常死亡
						log.Printf(" %v state change from follower to candidate\n", rf.me)
						rf.mu.Lock()
						rf.state = 1
						rf.timeout.Reset(rf.elapse)
						log.Printf(" %v timer was reset in natural and the elapse is %v", rf.me, rf.elapse)
						rf.mu.Unlock()
						rf.candidate <- 1
					}
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
				//select {
				//case <- rf.unnatural:{   //第一次成为某个term的leader
				//	for pos:= 0; pos < len(rf.peers); pos++{
				//		rf.nextIndex[pos] = rf.lastLogIndex+1
				//		rf.matchIndex[pos] = 0
				//	}
				//	log.Printf(" %v's initial nextIndex is %v", rf.me, rf.nextIndex)
				//}
				//default:{ //正常死亡
				//	log.Printf(" %v state keeps as leader\n", rf.me)
				//}
				//}
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
		sum := make([]int, len(peers))
		for {
			<-rf.candidate
			rf.mu.Lock()
			rf.currentTerm += 1
			rf.voteFor[rf.currentTerm] = rf.me
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
			for key := range sum {
				sum[key] = 0
			}
			sum[rf.me] = 1
			for i := 0; i < len(peers); i++ {
				if i != rf.me {
					i := i
					go func() {
						rf.mu.Lock()
						args := &RequestVoteArgs{rf.currentTerm, rf.me, len(rf.logOfRaft) - 1, rf.logOfRaft[len(rf.logOfRaft)-1].Term}
						reply := &RequestVoteReply{}
						log.Printf(" %v start send request to %v", rf.me, i)
						fterm := rf.currentTerm
						rf.mu.Unlock()
						if rf.sendRequestVote(i, args, reply) {
							if reply.VoteGranted {
								sum[i] = 1
							} else {
								if reply.Term > fterm && fterm == rf.currentTerm {
									rf.mu.Lock()

									rf.unnatural <- 1
									rf.currentTerm = reply.Term
									rf.state = 0
									rf.timeout.Reset(0)
									rf.mu.Unlock()
									//rf.unnatural <- 1
								}
							}
						}
					}()
				}
			}
			go func() {
				fterm := rf.currentTerm
				for {
					time.Sleep(10 * time.Millisecond) // 这个10ms是考虑从计时器开始计时到启动这个goroutine所用的开销
					num := 0
					for _, value := range sum {
						num += value
					}
					log.Printf(" %v get %v vote", rf.me, sum)
					if fterm == rf.currentTerm && rf.state == 1 {
						if num >= voteToGet {
							log.Printf(" %v was elected as leader", rf.me)
							rf.mu.Lock()
							for pos := 0; pos < len(rf.peers); pos++ {
								rf.nextIndex[pos] = len(rf.logOfRaft)
								rf.matchIndex[pos] = 0
							}
							rf.state = 2
							rf.timeout.Reset(0)
							rf.mu.Unlock()
							return
						}
					} else {
						return
					}
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
			for i := 0; i < len(peers); i++ {
				i := i
				if i != rf.me {
					go func() {
						rf.mu.Lock()
						args := &AppendEntriesArgs{rf.currentTerm, me, len(rf.logOfRaft) - 1, rf.logOfRaft[len(rf.logOfRaft)-1].Term, nil, rf.commitIndex}
						fterm := rf.currentTerm
						reply := &AppendEntriesReply{}
						rf.mu.Unlock()
						if rf.sendAppendEntries(i, args, reply) {
							log.Printf(" %v send heartbeat to %v success and the reply is %v", rf.me, i, reply)
							if !reply.Success {
								if reply.Term > fterm && fterm == rf.currentTerm && rf.state == 2 {
									rf.mu.Lock()
									rf.unnatural <- 1
									rf.currentTerm = reply.Term
									rf.state = 0
									rf.timeout.Reset(0)
									log.Printf(" %v from leader to follower\n", rf.me)
									rf.mu.Unlock()
									//rf.unnatural <- 1
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

	go func() {
		for {
			_commitIndex := <-rf.commitChannel
			log.Printf(" %v commit log %v ", rf.me, rf.logOfRaft[_commitIndex])
			applyCh <- ApplyMsg{true, rf.logOfRaft[_commitIndex].Command, _commitIndex}
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
