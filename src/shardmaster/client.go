package shardmaster

//
// Shardmaster clerk.
//

import (
	"labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	id        int64
	requestId int64
	mu        sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = nrand()
	ck.requestId = 0

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.

	ck.mu.Lock()
	args.CkId = ck.id
	ck.requestId += 1
	args.RequestId = ck.requestId
	ck.mu.Unlock()

	DPrintf("clerk need to query %v", num)

	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("clerk query %v and get result %v", num, reply.Config)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.mu.Lock()
	args.CkId = ck.id
	ck.requestId += 1
	args.RequestId = ck.requestId
	ck.mu.Unlock()

	DPrintf("clerk need to join %v", servers)

	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("clerk Join %v success", servers)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.mu.Lock()
	args.CkId = ck.id
	ck.requestId += 1
	args.RequestId = ck.requestId
	ck.mu.Unlock()

	DPrintf("clerk need to leave %v", gids)

	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("clerk leave %v success", gids)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.

	ck.mu.Lock()
	args.CkId = ck.id
	ck.requestId += 1
	args.RequestId = ck.requestId
	ck.mu.Unlock()

	DPrintf("clerk need to move shard(%v) to gid(%v)", shard, gid)

	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("clerk move shard(%v) to gid(%v) success", shard, gid)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
