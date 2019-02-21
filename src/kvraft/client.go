package raftkv

import (
	"labrpc"
	"log"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	currentLeader int
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
	ck.currentLeader = 0
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

	r := ""
	args := GetArgs{key}
	for {
		reply := GetReply{}
		if ok := ck.servers[ck.currentLeader].Call("KVServer.Get", &args, &reply); ok {
			if reply.WrongLeader == true {
				ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
				continue
			}
			DPrintf(" clerk get key(%v) from kvserver %v and the reply is %v, err IS %v", key, ck.currentLeader, reply.Value, reply.Err)
			if reply.Err != "" {
				r = ""
			} else {
				r = reply.Value
			}
			if reply.Err == ErrNoKey {
				r = ""
			} else if reply.Err == "" {
				r = reply.Value
				break
			} else {
				continue
			}
		}
	}

	return r
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
	args := PutAppendArgs{key, value, op}
	//reply := PutAppendReply{}
	for {
		reply := PutAppendReply{} //这个东西换个位置为什么会有那么大的影响，传的是指针不本来就要变化的吗
		ok := ck.servers[ck.currentLeader].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.WrongLeader == true {
				ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if op == PUT {
				DPrintf(" clerk put value(%v) to key(%v) from kvserver %v and the reply err IS%v", value, key, ck.currentLeader, reply.Err)
			} else if op == APPEND {
				DPrintf(" clerk append value(%v) to key(%v) from kvserver %v and the reply err IS%v", value, key, ck.currentLeader, reply.Err)
			} else {
				DPrintf(" clerk get wrong putAppend op")
			}
			if reply.Err != "" {
				log.Fatal(reply.Err)
			} else {
				break
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
