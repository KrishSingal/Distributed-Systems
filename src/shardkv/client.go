package shardkv

import (
	"crypto/rand"
	"math/big"
	"shardmaster"
	"strconv"
	"sync/atomic"
)
import "net/rpc"
import "time"
import "sync"
import "fmt"

type Clerk struct {
	mu     sync.Mutex // one RPC at a time
	sm     *shardmaster.Clerk
	config shardmaster.Config
	// You'll have to modify Clerk.
	transactionID int64
	clientID      int64
}

func MakeClerk(shardmasters []string) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(shardmasters)
	// You'll have to modify MakeClerk.
	ck.transactionID = 1
	// high probability unique client ID
	ck.clientID = nrand()
	return ck
}

// the nrand function same as pbservice
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigX, _ := rand.Int(rand.Reader, max)
	x := bigX.Int64()
	return x
}

// lock needed
var globalId int64 = 0

// generate unique request id:
// ClientID-TimeStamp-ClientMonotonicallyIncreasingTransactionID-
// HighProbabilityUniqueRandomTransactionID-GlobalID
func generateID() string {
	var ret string
	//ck.transactionID++
	atomic.AddInt64(&globalId, 1)
	ret = // strconv.FormatInt(ck.clientID, 10) + "-" +
		time.Now().String() + "-" +
			// strconv.FormatInt(ck.transactionID, 10) + "-" +
			strconv.FormatInt(nrand(), 10) + "-" +
			strconv.FormatInt(globalId, 10)
	return ret
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	// You'll have to modify Get().
	args := &GetArgs{}
	args.Key = key
	args.RequestID = generateID()

	for {
		shard := key2shard(key)

		gid := ck.config.Shards[shard]

		servers, ok := ck.config.Groups[gid]

		if ok {
			// try each server in the shard's replication group.
			for _, srv := range servers {

				var reply GetReply
				ok := call(srv, "ShardKV.Get", args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)

		// ask master for a new configuration.
		ck.config = ck.sm.Query(-1)
	}
	return ""
}

func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	// You'll have to modify Put().
	args := &PutArgs{}
	args.Key = key
	args.Value = value
	args.DoHash = dohash
	args.RequestID = generateID()

	for {
		shard := key2shard(key)

		gid := ck.config.Shards[shard]

		servers, ok := ck.config.Groups[gid]

		if ok {
			// try each server in the shard's replication group.
			for _, srv := range servers {

				var reply PutReply
				ok := call(srv, "ShardKV.Put", args, &reply)
				if ok && reply.Err == OK {
					return reply.PreviousValue
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)

		// ask master for a new configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
