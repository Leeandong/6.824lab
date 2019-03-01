package raftkv

import (
	"labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	//currentLeader int
	id        int64
	requestID int64
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
	// You'll have to add code here.
	//ck.currentLeader = 0
	ck.id = nrand()
	ck.requestID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	ck.requestID += 1
	ck.mu.Unlock()

	args := GetArgs{key, ck.id, ck.requestID}
	DPrintf(" clerk receive the request to get and args is %v and current kvservers are %v", args, ck.servers)
	for {
		for _, v := range ck.servers {
			reply := GetReply{}
			ok := v.Call("KVServer.Get", &args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf(" clerk get key(%v) and the reply is %v, err is %v", key, reply.Value, reply.Err)

				if reply.Err == ErrNoKey {
					return ""
				} else if reply.Err == OK {
					return reply.Value
				} else {
					DPrintf(" Error(%v) occurs in get ", reply.Err)

				}
			}
		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.mu.Lock()
	ck.requestID += 1
	ck.mu.Unlock()

	args := PutAppendArgs{key, value, op, ck.id, ck.requestID}
	DPrintf(" clerk receive the request to PutAppend and args is %v", args)

	for {
		for _, v := range ck.servers {
			reply := PutAppendReply{}
			ok := v.Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.WrongLeader == false {
				if op == PUT {
					DPrintf(" clerk put value(%v) to key(%v) and the reply err is %v", value, key, reply.Err)
				} else if op == APPEND {
					DPrintf(" clerk append value(%v) to key(%v) and the reply err is %v", value, key, reply.Err)
				} else {
					DPrintf(" clerk get wrong putAppend op")
				}

				if reply.Err == OK {
					return
				} else {
					DPrintf(" Error(%v) occurs in put", reply.Err)
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
