package kvpaxos

import (
	"net"
	"strconv"
	"time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string // nil if operation == "get"
	ID        int64
	DoHash    bool // nil if operation == "get"
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	database     map[string]string
	done         int
	RequestCache map[int64]bool
}

/*func (kv *KVPaxos) updateDatabase(min int, max int) {
	for i := min; i <= max; i++ {
		decided, value := kv.px.Status(i)
		if decided && value.Operation == "Put" {
			kv.database[value.Key] = value.Value
		}
	}
	kv.px.Done(max)
	kv.done = max
}*/

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// fmt.Printf("I am %d, and I received a Get() request for %s. My done value is %d\n", kv.me, args.Key, kv.done)
	seq := kv.done // initial sequence to try
	// kv.mu.Unlock()

	getOp := Op{}
	getOp.Operation = "Get"
	getOp.ID = args.ID
	getOp.Key = args.Key

	// kv.mu.Lock()

	for !kv.dead && !kv.RequestCache[args.ID] {
		// kv.mu.Unlock()
		seq += 1
		kv.px.Start(seq, getOp) // start the put operation

		timeout := 10 * time.Millisecond
		for !kv.dead { // wait for consensus to be reached
			decided, value := kv.px.Status(seq)
			// OpValue := value.(Op)
			if decided {
				// kv.mu.Lock()
				if value.(Op).Operation == "Put" { // update my database as we go
					if value.(Op).DoHash {
						prevVal := kv.database[value.(Op).Key]
						newVal := (int)(hash(prevVal + value.(Op).Value))
						kv.database[value.(Op).Key] = strconv.Itoa(newVal)
					} else {
						kv.database[value.(Op).Key] = value.(Op).Value
					}
				}
				kv.RequestCache[value.(Op).ID] = true
				kv.done = seq
				// kv.mu.Unlock()

				if value == getOp { // if my proposal went through, I'm done
					reply.Err = OK

					// kv.mu.Lock()
					reply.Value = kv.database[args.Key]
					// fmt.Printf("I am %d, and I am calling done on %d\n", kv.me, kv.done)
					kv.px.Done(kv.done)
					// kv.mu.Unlock()
					return nil
				} else { // otherwise, it was already decided, and I should try the next sequence number
					break
				}
			}
			time.Sleep(timeout)
			if timeout < 10*time.Second {
				timeout *= 2
			}
		}
		// kv.mu.Lock()
	}
	// kv.mu.Unlock()
	reply.Err = ErrDuplicateRequest
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// fmt.Printf("I am %d, and I received a Put() request for %s, %s. My done value is %d\n", kv.me, args.Key, args.Value, kv.done)
	seq := kv.done // initial sequence to try
	// kv.mu.Unlock()

	putOp := Op{}
	putOp.Operation = "Put"
	putOp.Key = args.Key
	putOp.ID = args.ID
	putOp.DoHash = args.DoHash
	putOp.Value = args.Value

	// kv.mu.Lock()

	for !kv.dead && !kv.RequestCache[args.ID] {
		// kv.mu.Unlock()
		seq += 1
		kv.px.Start(seq, putOp) // start the put operation

		timeout := 10 * time.Millisecond
		for !kv.dead { // wait for consensus to be reached
			decided, value := kv.px.Status(seq)
			// OpValue := value.(Op)
			if decided {
				// kv.mu.Lock()
				prevVal := ""
				if value.(Op).Operation == "Put" { // update my database as we go
					if value.(Op).DoHash {
						prevVal = kv.database[value.(Op).Key]
						newVal := (int)(hash(prevVal + value.(Op).Value))
						kv.database[value.(Op).Key] = strconv.Itoa(newVal)
					} else {
						kv.database[value.(Op).Key] = value.(Op).Value
					}
				}
				kv.RequestCache[value.(Op).ID] = true
				kv.done = seq
				// kv.mu.Unlock()

				if value == putOp { // if my proposal went through, I'm done
					reply.Err = OK
					reply.PreviousValue = prevVal

					// kv.mu.Lock()
					// fmt.Printf("I am %d, and I am calling done on %d\n", kv.me, kv.done)
					kv.px.Done(kv.done)
					// kv.mu.Unlock()

					return nil
				} else { // otherwise, it was already decided, and I should try the next sequence number
					break
				}
			}
			time.Sleep(timeout)
			if timeout < 1*time.Second {
				timeout *= 2
			}
		}
		// kv.mu.Lock()
	}
	//kv.mu.Unlock()
	reply.Err = ErrDuplicateRequest
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.database = make(map[string]string)
	kv.done = -1
	kv.RequestCache = make(map[int64]bool)

	// Your initialization code here.
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
