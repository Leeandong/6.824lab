package shardmaster

import (
	"log"
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs    []Config // indexed by config num
	commandSet map[int64]int64
	result     map[int]chan Id
}

type Op struct {
	// Your data here.
	Operation string
	Shard     int
	Num       int //desired config number
	GIDs      []int
	CkId      int64
	Servers   map[int][]string
	RequestId int64
}

type Id struct {
	CkId      int64
	RequestId int64
}

func dropAndSend(ch chan Id, operation Id) {
	select {
	case <-ch:
	default:
	}
	ch <- operation
}

func (sm *ShardMaster) AppendEntryToLog(command Op) bool {
	index, _, ok := sm.rf.Start(command)

	if ok == false {
		return false
	}

	DPrintf(" smServer(%v) put command[%v] (%v) to server %v", sm.me, index, command, sm.me)

	_id := Id{RequestId: command.RequestId,
		CkId: command.CkId}

	sm.mu.Lock()
	sm.result[index] = make(chan Id, 1)
	chanMsg := sm.result[index]
	sm.mu.Unlock()

	select {
	case <-time.After(time.Second):
		return false
	case r := <-chanMsg:
		return r == _id
	}

}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	command := Op{Operation: JOIN, Servers: args.Servers, CkId: args.CkId, RequestId: args.RequestId}
	if sm.AppendEntryToLog(command) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command := Op{Operation: LEAVE, GIDs: args.GIDs, CkId: args.CkId, RequestId: args.RequestId}
	if sm.AppendEntryToLog(command) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	_gids := make([]int, 1)
	_gids[0] = args.GID

	command := Op{Operation: MOVE, GIDs: _gids, Shard: args.Shard, CkId: args.CkId, RequestId: args.RequestId}
	if sm.AppendEntryToLog(command) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	command := Op{Operation: QUERY, Num: args.Num, CkId: args.CkId, RequestId: args.RequestId}
	if sm.AppendEntryToLog(command) {
		reply.WrongLeader = false
		sm.mu.Lock()
		if command.Num == -1 || command.Num > len(sm.configs)-1 {
			reply.Config = sm.configs[len(sm.configs)-1]
		} else {
			reply.Config = sm.configs[command.Num]
		}
		sm.mu.Unlock()
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

//type Config struct {
//	Num    int              // config number
//	Shards [NShards]int     // shard -> gid
//	Groups map[int][]string // gid -> servers[]
//}

func getMinElement(m map[int]int) (int, int) {
	minVal := NShards + 1
	var minKey int
	for k, v := range m {
		if v < minVal {
			minKey = k
			minVal = v
		}
	}
	return minKey, minVal
}

func getMaxElement(m map[int]int) (int, int) {
	maxVal := -1
	var maxKey int
	for k, v := range m {
		if v > maxVal {
			maxKey = k
			maxVal = v
		}
	}
	return maxKey, maxVal
}

//对shards的负载重新分配
//1.创建一个 gid 所拥有的 shard数目和 gid所拥有的 shard条目的map，进行统计。
//2.考虑没有被分配gid的shard， 例如其gid刚刚 leave了，给其分配gid。
//3. 进行负载均衡，基本思路是先找出拥有最多shard的gid，和最少shard的gid，若max-min<=1则满足
// 否则，将拥有最多shard的gid 分一个给则找出拥有最少 shard的gid	.
func (sm *ShardMaster) ReBalanceShards(config *Config) {
	numGid := len(config.Groups)
	shardOfGid := make(map[int][]int, numGid)  //gid所拥有的shard
	numShardOfGid := make(map[int]int, numGid) //gid拥有的 shard数目
	for k := range config.Groups {
		numShardOfGid[k] = 0
		shardOfGid[k] = make([]int, 0)
	}

	// config.Shards[] = -1 表示其没有被分配给gid

	for i, v := range config.Shards { //统计每一个 gid所拥有的shard
		if v != 0 {
			shardOfGid[v] = append(shardOfGid[v], i)
			numShardOfGid[v] += 1
		}
	}

	for i, v := range config.Shards { //找到没有分配的gid分配
		if v == 0 {
			minKey, _ := getMinElement(numShardOfGid)
			config.Shards[i] = minKey

			shardOfGid[minKey] = append(shardOfGid[minKey], i)
			numShardOfGid[minKey] += 1
		}
	}

	if numGid == 0 { //防止初始化时候numgid 为0， 下面计算average时分母为0。
		return
	}

	//这个分配居然数错了，之前是 average = NShards / numGid -.-
	//这样在有三个端口的时候下面的大循环死也出不去，4怎么会小于3呢 -.-
	//var average int
	//if NShards % numGid == 0{
	//	average = NShards / numGid
	//}else{
	//	average = NShards / numGid + 1
	//}
	//上面这个分发也有问题  ， 10/3的时候 ,4,4,2可以通过，但这个分配并不均匀
	//取最大值和最小值，当他们只差1的时候就满足了均匀的条件

	for {
		maxKey, maxVal := getMaxElement(numShardOfGid)
		minKey, minVal := getMinElement(numShardOfGid)

		if maxVal-minVal <= 1 {
			break
		} else {
			//minKey, _ := getMinElement(numShardOfGid)
			convertShards := shardOfGid[maxKey][0] //需要转移的shards
			shardOfGid[maxKey] = shardOfGid[maxKey][1:]
			numShardOfGid[maxKey] -= 1

			config.Shards[convertShards] = minKey

			shardOfGid[minKey] = append(shardOfGid[minKey], convertShards)
			numShardOfGid[minKey] += 1
		}
	}

}

func (sm *ShardMaster) GetNextConfig() Config {
	var c Config
	lastConfig := sm.configs[len(sm.configs)-1]
	c.Num = lastConfig.Num + 1
	c.Shards = lastConfig.Shards
	c.Groups = map[int][]string{}
	for k, v := range lastConfig.Groups {
		c.Groups[k] = v
	}
	return c
}

func (sm *ShardMaster) ApplyJoin(args JoinArgs) {
	_newConfig := sm.GetNextConfig()
	for k, v := range args.Servers {
		_newConfig.Groups[k] = v
	}
	sm.ReBalanceShards(&_newConfig)
	sm.configs = append(sm.configs, _newConfig)
}

func (sm *ShardMaster) ApplyLeave(args LeaveArgs) {
	_newConfig := sm.GetNextConfig()
	for _, toBeDeleteGids := range args.GIDs {
		delete(_newConfig.Groups, toBeDeleteGids)
		for k, v := range _newConfig.Shards {
			if v == toBeDeleteGids {
				_newConfig.Shards[k] = 0
			}
		}
	}
	sm.ReBalanceShards(&_newConfig)
	sm.configs = append(sm.configs, _newConfig)
}

func (sm *ShardMaster) ApplyMove(args MoveArgs) {
	_newConfig := sm.GetNextConfig()
	_newConfig.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, _newConfig)
}

func (sm *ShardMaster) ApplyToServer() {
	for {
		_commandToApply := <-sm.applyCh
		_commandIndex := _commandToApply.CommandIndex

		sm.mu.Lock()
		//DPrintf(" %v sm.mu.lock()", sm.me)

		if _commandToApply.Command != nil {
			_command := _commandToApply.Command.(Op)
			val, ok := sm.commandSet[_command.CkId]

			if !ok || (ok && val < _command.RequestId) {
				sm.commandSet[_command.CkId] = _command.RequestId

				if _command.Operation == JOIN {
					args := JoinArgs{Servers: _command.Servers}
					sm.ApplyJoin(args)

				} else if _command.Operation == LEAVE {
					args := LeaveArgs{GIDs: _command.GIDs}
					sm.ApplyLeave(args)

				} else if _command.Operation == MOVE {
					args := MoveArgs{Shard: _command.Shard, GID: _command.GIDs[0]}
					sm.ApplyMove(args)
				} else if _command.Operation == QUERY {

				} else {
					log.Fatal("operation is invalid")
				}

				if _command.Operation != QUERY {
					DPrintf("now sm server(%v) the config is %v", sm.me, sm.configs)
				}

			}

			ch, ok := sm.result[_commandIndex]
			if ok { //如果这条通道已经被建立，也就是这个kvServer对应的是leader，才需要把信号发出去
				dropAndSend(ch, Id{CkId: _command.CkId, RequestId: _command.RequestId})
				DPrintf("%v return commit log %v to clerk success", sm.me, _commandIndex)
			} else {
				DPrintf("%v return commit log %v to clerk false", sm.me, _commandIndex)
			}
		}
		sm.mu.Unlock()
		//DPrintf(" %v sm.mu.unlock()", sm.me)
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	sm.commandSet = make(map[int64]int64)
	sm.result = make(map[int]chan Id)

	go sm.ApplyToServer()

	return sm
}
