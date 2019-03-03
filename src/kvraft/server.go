package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "Get"
	APPEND = "Append"
	PUT    = "Put"
)

type Notice struct {
	index int
	err   Err
	val   string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Operation  string
	Key        string
	Value      string
	SequenceId int64
	CkId       int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	dataSet    map[string]string
	commandSet map[int64]int64
	result     map[int]chan Op

	//index      int   //记录当前所受到的最高的 apply index 的编号
}

func dropAndSend(ch chan Op, operation Op) {
	select {
	case <-ch:
	default:
	}
	ch <- operation
}

func (kv *KVServer) AppendEntryToLog(command Op) bool {
	index, _, ok := kv.rf.Start(command)

	if ok == false {
		return false
	}

	DPrintf(" kvServer put command[%v] (%v) to server %v", index, command, kv.me)

	kv.mu.Lock()
	kv.result[index] = make(chan Op, 1)
	chanMsg := kv.result[index]
	kv.mu.Unlock()

	select {
	case <-time.After(time.Second):
		return false
	case r := <-chanMsg:
		return r == command
	}

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	// Your code here.
	command := Op{GET, args.Key, "", args.RequestId, args.CkId}

	if kv.AppendEntryToLog(command) {
		kv.mu.Lock()
		val, ok := kv.dataSet[args.Key]
		kv.mu.Unlock()
		reply.WrongLeader = false
		if ok {
			reply.Value = val
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		reply.WrongLeader = true
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{args.Op, args.Key, args.Value, args.RequestId, args.CkId}
	if kv.AppendEntryToLog(command) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) ApplyToServer(persister *raft.Persister) {
	for {
		_commandToApply := <-kv.applyCh
		_commandIndex := _commandToApply.CommandIndex

		kv.mu.Lock()
		if _commandToApply.CommandValid == true {
			_command := _commandToApply.Command.(Op)
			//kv.mu.Lock()
			val, ok := kv.commandSet[_command.CkId]

			if !ok || (ok && val < _command.SequenceId) {
				kv.commandSet[_command.CkId] = _command.SequenceId
				if _command.Operation == PUT {
					kv.dataSet[_command.Key] = _command.Value
				} else if _command.Operation == APPEND {
					kv.dataSet[_command.Key] += _command.Value
				} else {

				}
			}

			ch, ok := kv.result[_commandIndex]
			if ok { //如果这条通道已经被建立，也就是这个kvServer对应的是leader，才需要把信号发出去
				dropAndSend(ch, _command)
			}
		} else {
			_snapShot := _commandToApply.Snapshot
			kv.readSnapShot(_snapShot)
		}
		kv.mu.Unlock()

		if (kv.maxraftstate != -1) && (kv.maxraftstate < persister.RaftStateSize()) {
			kv.mu.Lock()
			_snapShot := kv.SnapShot()
			kv.mu.Unlock()
			DPrintf(" %v snapshot success", kv.me)
			//kv.rf.Lock()
			kv.rf.WriteStateAndSnapShot(_snapShot, _commandIndex, -1)
			//kv.rf.Unlock()
		}
	}
}

//使用函数时需要加所锁
func (kv *KVServer) SnapShot() []byte {

	log.Printf("kv server %v install snapshot by itself", kv.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	err := e.Encode(kv.dataSet)
	if err != nil {
		log.Fatal(err)
	}

	err = e.Encode(kv.commandSet)
	if err != nil {
		log.Fatal(err)
	}

	data := w.Bytes()
	return data
}

func (kv *KVServer) readSnapShot(data []byte) {
	DPrintf(" kvServer %v read the snapshot", kv.me)

	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var dataset map[string]string
	var commandSet map[int64]int64

	err := d.Decode(&dataset)
	if err != nil {
		log.Fatal(err)
	}

	err = d.Decode(&commandSet)
	if err != nil {
		log.Fatal(err)
	}

	kv.dataSet = dataset
	kv.commandSet = commandSet

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dataSet = make(map[string]string)
	kv.commandSet = make(map[int64]int64)
	kv.result = make(map[int]chan Op)

	kv.mu.Lock()
	kv.readSnapShot(persister.ReadSnapshot())
	kv.mu.Unlock()

	go kv.ApplyToServer(persister)

	return kv
}
