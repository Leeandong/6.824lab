package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"reflect"
	"sync"
)

const Debug = 0

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

	Operation string
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	dataSet      map[string]string
	applyIndex   int
	notification chan Notice
}

func Drop(noticeChan chan Notice) {
	select {
	case <-noticeChan:
	default:
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	command := Op{GET, args.Key, ""}

	index, _, ok := kv.rf.Start(command)
	DPrintf(" the rfserver(%v) start command success? %v", kv.me, ok)
	if ok == false {
		reply.WrongLeader = true
		return
	}

	tmpNotice := <-kv.notification

	if tmpNotice.index == index {
		reply.Err = tmpNotice.err
		reply.Value = tmpNotice.val
		reply.WrongLeader = false
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	_op := args.Op
	_key := args.Key
	_value := args.Value

	command := Op{_op, _key, _value}

	index, _, ok := kv.rf.Start(command)
	DPrintf(" the rfserver(%v) start command success? %v", kv.me, ok)
	if ok == false {
		reply.WrongLeader = true
		return
	}

	tmpNotice := <-kv.notification

	//DPrintf( " %v get the notification and command index is %v, apply index is %v", kv.me, index, tmpNotice.index)

	if tmpNotice.index == index {
		//DPrintf( " %v reply %v is success ", kv.me, *reply)
		reply.Err = tmpNotice.err
		reply.WrongLeader = false
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

func (kv *KVServer) ConcreteGet(key string) (Err, string) {

	var _err Err = OK
	var _val string

	kv.mu.Lock()
	val, ok := kv.dataSet[key]
	kv.mu.Unlock()
	if ok {
		_val = val
	} else {
		_err = ErrNoKey
	}

	return _err, _val

}

func (kv *KVServer) ConcretePut(key string, val string) (Err, string) {

	var _err Err = OK
	var _val string

	kv.mu.Lock()
	_, ok := kv.dataSet[key]
	kv.mu.Unlock()
	if ok {
		kv.dataSet[key] = val
	} else {
		kv.dataSet[key] = val
	}

	return _err, _val

}

func (kv *KVServer) ConcreteAppend(key string, val string) (Err, string) {
	var _err Err = OK
	var _val string

	kv.mu.Lock()
	_, ok := kv.dataSet[key]
	kv.mu.Unlock()
	if ok {
		kv.dataSet[key] += val
	} else {
		kv.dataSet[key] = val
	}

	return _err, _val

}

func (kv *KVServer) ApplyToServer() {
	for {
		_commandToApply := <-kv.applyCh
		_operation := reflect.ValueOf(_commandToApply.Command)
		_op := _operation.FieldByName("Operation").String()
		_key := _operation.FieldByName("Key").String()
		_value := _operation.FieldByName("Value").String()

		_index := int(reflect.ValueOf(_commandToApply.CommandIndex).Int())

		var _notice Notice
		_notice.index = _index

		var _err Err
		var _val string

		switch _op {
		case GET:
			_err, _val = kv.ConcreteGet(_key)
		case PUT:
			_err, _val = kv.ConcretePut(_key, _value)
		case APPEND:
			_err, _val = kv.ConcreteAppend(_key, _value)
		}

		_notice.err = _err
		_notice.val = _val

		Drop(kv.notification)
		kv.notification <- _notice
	}
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
	kv.notification = make(chan Notice, 1)

	go kv.ApplyToServer()

	return kv
}
