package kvpaxos

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
)
import "fmt"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
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
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	getArgs := &GetArgs{}
	getArgs.Key = key
	getArgs.ID = nrand()
	getReply := &GetReply{}

	for {
		for _, server := range ck.servers {

			// fmt.Printf("Sending Get() to server %s\n", server)
			success := call(server, "KVPaxos.Get", getArgs, getReply)
			/*if success {
				fmt.Printf("getReply.Err for server %s: %s\n", server, getReply.Err)
			} else {
				fmt.Printf("Get() for server %s did not succeed", server)
			}*/
			if success && getReply.Err == OK {
				return getReply.Value
			}
		}
	}
	return ""
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	// You will have to modify this function.
	putArgs := &PutArgs{}
	putArgs.Key = key
	putArgs.Value = value
	putArgs.DoHash = dohash
	putArgs.ID = nrand()
	putReply := &PutReply{}

	for {
		for _, server := range ck.servers {

			// fmt.Printf("Sending Put() to server %s\n", server)
			success := call(server, "KVPaxos.Put", putArgs, putReply)
			/*if success {
				fmt.Printf("putReply.Err for server %s: %s\n", server, putReply.Err)
			} else {
				fmt.Printf("Put() for server %s did not succeed", server)
			}*/
			if success && putReply.Err == OK {
				return putReply.PreviousValue
			}
		}
	}
	return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
