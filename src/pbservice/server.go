package pbservice

import (
	"net"
	"strconv"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	// viewnum       uint
	view          viewservice.View
	kv_store      map[string]string
	request_cache map[int64]bool
	initialized   bool
	mu            sync.Mutex
	IDtoPrev      map[int64]string
	//backup        bool
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {

	pb.mu.Lock()

	// If I do not think I am primary based on the current view I have, return err
	// fmt.Printf("I am pbserver: %s, and I see the view as %v\n", pb.me, pb.view)
	// fmt.Printf("I am %s, and my kv_store is %v\n", pb.me, pb.kv_store)

	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.request_cache[args.RequestID] {
		reply.Err = ErrDuplicateRequest
		reply.PreviousValue = pb.IDtoPrev[args.RequestID]
		return nil
	}

	pb.mu.Unlock()

	// fmt.Printf("I am pbserver: %s, and I am processing request %d\n", pb.me, args.RequestID)
	// fmt.Printf("The key is %s, and the value is %s\n", args.Key, args.Value)

	/*if pb.view.Backup == "" {
		if args.DoHash {
			prevVal := pb.kv_store[args.Key]

			reply.PreviousValue = prevVal
			pb.IDtoPrev[args.RequestID] = prevVal

			newVal := (int)(hash(prevVal + args.Value))
			pb.kv_store[args.Key] = strconv.Itoa(newVal)
		} else {
			pb.kv_store[args.Key] = args.Value
		}
		pb.request_cache[args.RequestID] = true
		reply.Err = OK
		return nil
	}*/

	// forward to backup
	forwardArgs := &ForwardArgs{}
	forwardArgs.ClaimedPrimary = pb.me
	forwardArgs.Operation = "Put"
	forwardArgs.Key = args.Key
	forwardArgs.Value = args.Value
	forwardArgs.DoHash = args.DoHash
	forwardArgs.RequestID = args.RequestID

	forwardReply := &ForwardReply{}

	forwardSuccess := false
	i := 0

	pb.mu.Lock()
	currBackup := pb.view.Backup
	pb.mu.Unlock()

	for !forwardSuccess && currBackup != "" {
		if i%100_000 == 0 {
			// fmt.Printf("Backup is %s\n", currBackup)
		}
		i++

		forwardReply = &ForwardReply{}
		forwardSuccess = call(currBackup, "PBServer.Forward", forwardArgs, forwardReply)

		pb.mu.Lock()
		currBackup = pb.view.Backup
		pb.mu.Unlock()
	}

	// if backup acks that I am primary, then we're good
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup == "" || forwardReply.Err == OK {
		if args.DoHash {
			prevVal := pb.kv_store[args.Key]

			reply.PreviousValue = prevVal
			pb.IDtoPrev[args.RequestID] = prevVal

			newVal := (int)(hash(prevVal + args.Value))
			pb.kv_store[args.Key] = strconv.Itoa(newVal)
		} else {
			pb.kv_store[args.Key] = args.Value
		}
		pb.request_cache[args.RequestID] = true
		reply.Err = OK
	} else { // otherwise, return error
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// If I do not think I am primary based on the current view I have, return err
	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.request_cache[args.RequestID] {
		reply.Err = ErrDuplicateRequest
		return nil
	}

	// fmt.Printf("current backup:%v\n", pb.view.Backup)
	// fmt.Printf("backup == \"\" %t", pb.view.Backup == "")

	if pb.view.Backup == "" {
		reply.Value = pb.kv_store[args.Key]

		if reply.Value == "" {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
		}
		return nil
	}

	// forward to backup
	forwardArgs := &ForwardArgs{}
	forwardArgs.ClaimedPrimary = pb.me
	forwardArgs.Operation = "Get"
	forwardArgs.Key = args.Key
	forwardArgs.RequestID = args.RequestID

	forwardReply := &ForwardReply{}

	forwardSuccess := false
	for !forwardSuccess {
		forwardReply = &ForwardReply{}
		forwardSuccess = call(pb.view.Backup, "PBServer.Forward", forwardArgs, forwardReply)
	}

	// if backup acks that I am primary, then we're good
	if forwardReply.Err == OK {
		reply.Value = pb.kv_store[args.Key]

		if reply.Value == "" {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
		}
	} else { // otherwise, return error
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) Initialize(args *InitializationArgs, reply *InitializationReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.Me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return nil
	}
	//fmt.Printf("I am %s, and I am giving the kv_store %v to %s\n", pb.me, pb.kv_store, args.Me)

	reply.Err = OK
	pb.kv_store = args.KVStore
	pb.request_cache = args.RequestCache
	pb.initialized = true
	// reply.KVStore = pb.kv_store
	// reply.RequestCache = pb.request_cache

	//fmt.Printf("the kv_store in the initializationReply struct is %v\n", reply.KVStore)

	return nil
}

func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {
	for !pb.initialized {
	}

	pb.mu.Lock()
	defer pb.mu.Unlock()

	// fmt.Printf("I am %s, in forward. I see the view as %v\n", pb.me, pb.view)
	claimed_primary := args.ClaimedPrimary

	if claimed_primary == pb.view.Primary {
		if args.Operation == "Put" {
			if args.DoHash {
				prevVal := pb.kv_store[args.Key]
				newVal := (int)(hash(prevVal + args.Value))
				pb.kv_store[args.Key] = strconv.Itoa(newVal)
			} else {
				//fmt.Printf("I am %s\n", pb.me)
				//fmt.Printf("kv_store %v\n", pb.kv_store)
				//fmt.Printf("kv_store[%s]: %v\n", args.Key, pb.kv_store[args.Key])
				pb.kv_store[args.Key] = args.Value
			}
			pb.request_cache[args.RequestID] = true
		}
		reply.Err = OK
		// fmt.Printf("%s accepted the forward. The kv_store is now %v\n", pb.me, pb.kv_store)
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	prevBackup := pb.view.Backup
	//fmt.Printf("I am %s, and my kv_store is %v\n", pb.me, pb.kv_store)
	// fmt.Printf("I am pbserver: %s, and I am trying to ping during my tick\n", pb.me)
	view, _ := pb.vs.Ping(pb.view.Viewnum)
	pb.view = view
	newBackup := pb.view.Backup
	//fmt.Printf("I am pbserver: %s, and I see the view as %v\n", pb.me, pb.view)
	// not using error value here. Might need to fix this
	// update view

	if pb.view.Primary == pb.me && newBackup != "" && newBackup != prevBackup {
		initializationArgs := &InitializationArgs{}
		initializationArgs.Me = pb.me
		initializationArgs.KVStore = pb.kv_store
		initializationArgs.RequestCache = pb.request_cache
		initializationReply := &InitializationReply{}

		initializationSuccess := false
		for !initializationSuccess {
			initializationReply = &InitializationReply{}
			//fmt.Printf("initializationReply is %v\n", initializationReply)
			initializationSuccess = call(pb.view.Backup, "PBServer.Initialize", initializationArgs, initializationReply)
		}
	}

	/*if pb.view.Backup == pb.me && pb.backup == false {
		// repeatedly call initialization until it works and I get a copy
		initializationArgs := &InitializationArgs{}
		initializationArgs.Me = pb.me
		initializationReply := &InitializationReply{}

		initializationSuccess := false
		for !initializationSuccess {
			initializationReply = &InitializationReply{}
			//fmt.Printf("initializationReply is %v\n", initializationReply)
			initializationSuccess = call(pb.view.Primary, "PBServer.Initialize", initializationArgs, initializationReply)
		}

		//fmt.Printf("I am %s, and I am trying to initialize\n", pb.me)
		//fmt.Printf("Initialization reply is %v\n", initializationReply)
		if initializationReply.Err == OK {
			fmt.Printf("I am %s, and I got kv_store %v\n", pb.me, (*initializationReply).KVStore)
			//fmt.Printf("I am %s, and I got request_cache %v\n", pb.me, (*initializationReply).RequestCache)

			pb.kv_store = initializationReply.KVStore

			pb.request_cache = initializationReply.RequestCache
		}

		initializationSuccess := false
		for !initializationSuccess && initializationReply.Err != OK {
			initializationReply = &InitializationReply{}
			//fmt.Printf("initializationReply is %v\n", initializationReply)
			initializationSuccess = call(pb.view.Primary, "PBServer.Initialize", initializationArgs, initializationReply)
		}

		pb.kv_store = initializationReply.KVStore
		pb.request_cache = initializationReply.RequestCache

	}*/
	// if I just got promoted to backup, then call Initialization() on primary to get the kv store and request cache

	//pb.backup = (pb.view.Backup == pb.me)

	if pb.me != pb.view.Backup && pb.me != pb.view.Primary {
		pb.initialized = false
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.view = viewservice.View{}
	pb.initialized = false
	pb.kv_store = make(map[string]string)
	pb.request_cache = make(map[int64]bool)
	pb.IDtoPrev = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			// fmt.Printf("I am pbservice: %s, and I am calling tick()\n", pb.me)
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
