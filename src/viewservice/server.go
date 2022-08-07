package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here
	pingTimes        map[string]time.Time
	view             View
	acked            bool
	active           map[string]bool // just for unused servers
	pingValues       map[string]uint
	primary_activity bool
	backup_activity  bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	vs.mu.Lock()
	// Your code here.
	client := args.Me
	/*
		if vs.view.Viewnum == 0 {
			vs.view.Primary = client
			vs.view.Viewnum++
		}
	*/

	vs.pingTimes[client] = time.Now()    // update ping timing
	vs.pingValues[client] = args.Viewnum // update last ping value
	if client != vs.view.Primary && client != vs.view.Backup {
		vs.active[client] = true
	} else if client == vs.view.Primary {
		vs.primary_activity = true
	} else {
		vs.backup_activity = true
	}
	reply.View = vs.view

	if client == vs.view.Primary && args.Viewnum == vs.view.Viewnum {
		vs.acked = true // check for proper ack from primary
		// fmt.Println(client, vs.view.Viewnum)
	}

	vs.mu.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.view
	vs.mu.Unlock()

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// fmt.Println(vs.view)
	// fmt.Println(vs.acked)

	vs.mu.Lock()

	for server, _ := range vs.active {
		pingTime := vs.pingTimes[server]
		if time.Until(pingTime) >= DeadPings*PingInterval {
			delete(vs.active, server)
		}
	}

	primary := vs.view.Primary
	backup := vs.view.Backup

	// fmt.Println(time.Until(vs.pingTimes[primary]))
	// fmt.Println(DeadPings * PingInterval)
	if -time.Until(vs.pingTimes[primary]) >= DeadPings*PingInterval {
		vs.primary_activity = false
	} else {
		vs.primary_activity = true
	}
	if -time.Until(vs.pingTimes[backup]) >= DeadPings*PingInterval {
		vs.backup_activity = false
	} else {
		vs.backup_activity = true
	}

	// fmt.Println("Primary Activity:", vs.view.Primary, vs.primary_activity)
	// fmt.Println("Backup Activity:", vs.view.Backup, vs.backup_activity)

	changed := false

	if vs.view.Viewnum == 0 {
		for server, _ := range vs.active { // hacky way to get a primary server
			vs.view.Primary = server
			delete(vs.active, server)
			changed = true
			break
		}
	}

	if (!vs.primary_activity || vs.pingValues[primary] == 0) && vs.acked { //if primary isn't active any longer, we upgrade backup
		// fmt.Println("Primary activity", vs.primary_activity)
		// pingTime := vs.pingTimes[primary]
		// fmt.Println("Ping time", time.Until(pingTime))
		vs.view.Primary = backup
		vs.view.Backup = ""
		changed = true
	}

	if (backup == "" || !vs.backup_activity) && vs.acked {
		vs.view.Backup = ""
		for server, _ := range vs.active { // hacky way to get a backup server
			vs.view.Backup = server
			delete(vs.active, server)
			changed = true
			break
		}
	}

	if changed {
		// fmt.Println(vs.acked)
		vs.view.Viewnum++
		vs.acked = false
		// fmt.Println(vs.view)
		// fmt.Println(vs.acked)
	}

	vs.mu.Unlock()
	// Your code here.
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.dead = false
	vs.acked = false
	vs.pingTimes = make(map[string]time.Time)
	vs.pingValues = make(map[string]uint)
	vs.active = make(map[string]bool)
	// Your vs.* initializations here.

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
