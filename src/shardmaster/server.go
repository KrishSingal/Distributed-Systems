package shardmaster

import (
	"net"
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

// import "crypto/rand"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs   []Config // indexed by config num
	done      int
	joined    map[int64]bool
	left      map[int64]bool
	workload  map[int64][]int64
	configNum int
}

type Op struct {
	// Your data here.
	Operation string
	Config    Config
	GID       int64
	Servers   []string
	QueryID   int64
}

const (
	OK     = "OK"
	Reject = "Reject"
)

type Err string

func Max(a int64, b int64) int64 {
	if a >= b {
		return a
	} else {
		return b
	}
}

func Min(a int64, b int64) int64 {
	if a <= b {
		return a
	} else {
		return b
	}
}

func (sm *ShardMaster) Construct(config Config) {
	sm.workload = make(map[int64][]int64)
	for k, _ := range config.Groups {
		sm.workload[k] = []int64{}
	}

	for i := 0; i < NShards; i++ {
		if config.Shards[i] > 0 {
			sm.workload[config.Shards[i]] = append(sm.workload[config.Shards[i]], int64(i))
		}
	}
}

func (sm *ShardMaster) Largest() (int64, int64) {
	var max_value int64 = 0
	var max_key int64 = -1
	for k, v := range sm.workload {
		if int64(len(v)) >= max_value {
			max_key = k
			max_value = int64(len(v))
		}
	}

	return max_key, max_value
}

func (sm *ShardMaster) Smallest() (int64, int64) {
	var min_value int64 = NShards
	var min_key int64 = -1
	for k, v := range sm.workload {
		if int64(len(v)) <= min_value {
			min_key = k
			min_value = int64(len(v))
		}
	}

	return min_key, min_value
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.

	/*
		oldConfig := Config{}
		oldConfig.Num = 0
		oldConfig.Shards = [NShards]int64{}
		oldConfig.Groups = make(map[int64][]string)
		if sm.done != 0 {
			_, value := sm.px.status(sm.done) // assume decided
			oldConfig = value.(Config)
		}
	*/

	sm.mu.Lock()
	// fmt.Printf("In Join(), took lock")
	defer sm.mu.Unlock()

	if sm.joined[args.GID] {
		return nil
	}

	// oldConfig := sm.configs[sm.done]

	joinOp := Op{}
	joinOp.Operation = "Join"
	joinOp.GID = args.GID
	joinOp.Servers = args.Servers
	joinOp.Config = Config{}

	// joinOp.Config.Num = sm.done + 1

	/*joinOp.Config.Groups = make(map[int64][]string) // build new group map
	for k, v := range oldConfig.Groups {
		joinOp.Config.Groups[k] = v
	}
	joinOp.Config.Groups[args.GID] = args.Servers

	GIDs := make([]int64, len(oldConfig.Groups)+1)
	i := 0
	for k, _ := range joinOp.Config.Groups {
		GIDs[i] = k
		i++
	}

	// re-assign shards
	joinOp.Config.Shards = [NShards]int64{}
	for j := 0; j < NShards; j++ {
		joinOp.Config.Shards[j] = GIDs[j%len(joinOp.Config.Groups)]
	}*/

	seq := sm.done

	for !sm.dead {
		seq += 1
		joinOp.Config.Num = sm.configNum + 1 // update config number with new paxos log index number

		// Building Groups and assigning Shards
		oldConfig := sm.configs[sm.configNum]
		joinOp.Config.Groups = make(map[int64][]string) // build new group map
		for k, v := range oldConfig.Groups {
			joinOp.Config.Groups[k] = v
		}
		joinOp.Config.Groups[args.GID] = args.Servers

		sm.Construct(oldConfig)
		if len(sm.workload) == 0 {
			sm.workload[args.GID] = []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
			for i := 0; i < NShards; i++ {
				joinOp.Config.Shards[i] = args.GID
			}
		} else {
			// need to construct workload from the old config
			// sm.Construct(oldConfig)
			// populate 'shards' in op struct with oldconfig
			for i := 0; i < NShards; i++ {
				joinOp.Config.Shards[i] = oldConfig.Shards[i]
			}
			sm.workload[args.GID] = []int64{}
			maxk, maxv := sm.Largest()
			mink, minv := sm.Smallest()
			//fmt.Printf("%d, %d, %d, %d\n", maxk, maxv, mink, minv)

			for maxv > minv+1 {
				final_element := sm.workload[maxk][len(sm.workload[maxk])-1]
				sm.workload[maxk] = sm.workload[maxk][:len(sm.workload[maxk])-1]
				sm.workload[mink] = append(sm.workload[mink], final_element)

				joinOp.Config.Shards[final_element] = mink

				maxk, maxv = sm.Largest()
				mink, minv = sm.Smallest()
			}
		}
		// Done building groups and assigning shards

		sm.px.Start(seq, joinOp)
		timeout := 10 * time.Millisecond

		for !sm.dead {
			dec, val := sm.px.Status(seq)
			if dec {
				sm.done = seq
				if val.(Op).Operation != "Query" {
					sm.configNum++
				}
				if val.(Op).GID == joinOp.GID && val.(Op).Operation == joinOp.Operation {
					//fmt.Printf("I am %d and %d successfully joined\n", sm.me, joinOp.GID)
					//fmt.Printf("config num %d, has shards %v\n", seq, joinOp.Config.Shards)
					//_, v := sm.px.Status(seq)
					//fmt.Printf("Paxos log has %v for seq %d\n", v.(Op).Config, seq)

					sm.joined[joinOp.GID] = true
					sm.configs = append(sm.configs, joinOp.Config)

					sm.px.Done(sm.done)
					return nil
				} else if val.(Op).Operation != "Query" {
					sm.configs = append(sm.configs, val.(Op).Config)
					break
				} else {
					break
				}
			}

			time.Sleep(timeout)
			if timeout < 10*time.Second {
				timeout *= 2
			}
		}
	}
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	/*oldConfig := Config{}
	oldConfig.Num = 0
	oldConfig.Shards = [NShards]int64{}
	oldConfig.Groups = make(map[int64][]string)
	if sm.done != 0 {
		_, value := sm.px.status(sm.done) // assume decided
		oldConfig = value.(Config)
	}*/

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.left[args.GID] {
		return nil
	}

	// oldConfig := sm.configs[sm.done]

	leaveOp := Op{}
	leaveOp.Operation = "Leave"
	leaveOp.GID = args.GID
	leaveOp.Config = Config{}
	// leaveOp.Config.Num = sm.done + 1

	/*leaveOp.Config.Groups = make(map[int64][]string) // build new group map
	for k, v := range oldConfig.Groups {
		leaveOp.Config.Groups[k] = v
	}
	delete(leaveOp.Config.Groups, leaveOp.GID)

	GIDs := make([]int64, len(leaveOp.Config.Groups))
	i := 0
	for k, _ := range leaveOp.Config.Groups {
		GIDs[i] = k
		i++
	}

	// re-assign shards
	leaveOp.Config.Shards = [NShards]int64{}
	i = 0
	for j := 0; j < NShards; j++ {
		if oldConfig.Shards[j] == leaveOp.GID {
			leaveOp.Config.Shards[j] = GIDs[i%len(leaveOp.Config.Groups)]
			i++
		} else {
			leaveOp.Config.Shards[j] = oldConfig.Shards[j]
		}
	}*/

	seq := sm.done

	for !sm.dead {
		seq += 1
		leaveOp.Config.Num = sm.configNum + 1 // update config number with new paxos log index number

		// Building map and assigning shards
		oldConfig := sm.configs[sm.configNum]

		leaveOp.Config.Groups = make(map[int64][]string) // build new group map
		for k, v := range oldConfig.Groups {
			leaveOp.Config.Groups[k] = v
		}
		delete(leaveOp.Config.Groups, leaveOp.GID)

		// construct table
		sm.Construct(oldConfig)
		//fmt.Printf("oldConfig is: %v. worklaod is :%v\n", oldConfig, sm.workload)
		// populate 'shards' in op struct with oldconfig
		for i := 0; i < NShards; i++ {
			leaveOp.Config.Shards[i] = oldConfig.Shards[i]
		}

		workload_tbr := sm.workload[leaveOp.GID]
		delete(sm.workload, leaveOp.GID)
		//fmt.Printf("Need GID %d to leave. workload to be removed: %v\n", leaveOp.GID, workload_tbr)

		for i := 0; i < len(workload_tbr); i++ {
			mink, _ := sm.Smallest()
			//fmt.Printf("mink: %d, minv: %d\n", mink, minv)
			shard := workload_tbr[i]
			sm.workload[mink] = append(sm.workload[mink], shard)

			leaveOp.Config.Shards[shard] = mink
		}
		// Done Building map and assigning shards

		sm.px.Start(seq, leaveOp)

		timeout := 10 * time.Millisecond
		for !sm.dead {
			dec, val := sm.px.Status(seq)
			if dec {
				sm.done = seq
				if val.(Op).Operation != "Query" {
					sm.configNum++
				}
				if val.(Op).GID == leaveOp.GID && val.(Op).Operation == leaveOp.Operation {
					//fmt.Printf("I am %d and %d successfully left\n", sm.me, leaveOp.GID)
					//fmt.Printf("config num %d, has shards %v\n", seq, leaveOp.Config.Shards)
					// _, v := sm.px.Status(seq)
					//fmt.Printf("Paxos log has %v for seq %d\n", val.(Op).Config, seq)

					sm.left[leaveOp.GID] = true
					sm.configs = append(sm.configs, leaveOp.Config)
					// sm.configs[sm.done] = leaveOp.Config
					sm.px.Done(sm.done)
					return nil
				} else if val.(Op).Operation != "Query" {
					sm.configs = append(sm.configs, val.(Op).Config)
					break
				} else {
					break
				}
			}

			time.Sleep(timeout)
			if timeout < 10*time.Second {
				timeout *= 2
			}
		}
	}
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// oldConfig := sm.configs[sm.configNum]

	moveOp := Op{}
	moveOp.Operation = "Move"
	moveOp.GID = args.GID
	moveOp.Config = Config{}
	// moveOp.Config.Num = sm.configNum + 1

	seq := sm.done

	for !sm.dead {
		seq += 1

		moveOp.Config.Num = sm.configNum + 1
		oldConfig := sm.configs[sm.configNum]

		moveOp.Config.Groups = make(map[int64][]string) // build new group map
		for k, v := range oldConfig.Groups {
			moveOp.Config.Groups[k] = v
		}

		for i := 0; i < len(oldConfig.Shards); i++ {
			moveOp.Config.Shards[i] = oldConfig.Shards[i]
		}
		moveOp.Config.Shards[args.Shard] = args.GID

		// moveOp.Config.Num = sm.configNum + 1 // update config number with new paxos log index number
		sm.px.Start(seq, moveOp)

		timeout := 10 * time.Millisecond
		for !sm.dead {
			dec, val := sm.px.Status(seq)
			if dec {
				sm.done = seq
				if val.(Op).Operation != "Query" {
					sm.configNum++
				}
				if val.(Op).GID == moveOp.GID && val.(Op).Operation == moveOp.Operation {
					sm.configs = append(sm.configs, moveOp.Config)
					// sm.configs[sm.done] = moveOp.Config
					return nil
				} else if val.(Op).Operation != "Query" {
					sm.configs = append(sm.configs, val.(Op).Config)
					break
				} else {
					break
				}
			}

			time.Sleep(timeout)
			if timeout < 10*time.Second {
				timeout *= 2
			}
		}
	}
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	// fmt.Printf("In Query(), waiting for lock\n")
	sm.mu.Lock()
	// fmt.Printf("In Query(), took lock\n")
	defer sm.mu.Unlock()

	//fmt.Printf("I am %d. done is: %d, and the latest config num is %d \n", sm.me, sm.done, len(sm.configs)-1)

	agreement := false
	// fmt.Printf("In Query(), done is %d\n", sm.done)
	// time.Sleep(10 * time.Second)
	queryOp := Op{}
	queryOp.Operation = "Query"
	queryOp.Config = Config{}
	queryOp.QueryID = nrand()

	seq := sm.done
	// queryOp.Config.Num = sm.done

	for !sm.dead && !agreement {
		seq += 1

		oldConfig := sm.configs[sm.configNum]
		queryOp.Config.Num = sm.configNum // update config number with new paxos log index number

		// fmt.Printf("oldConfig: %v\n", oldConfig)
		queryOp.Config.Groups = make(map[int64][]string) // build new group map
		for k, v := range oldConfig.Groups {
			queryOp.Config.Groups[k] = v
		}
		for i := 0; i < NShards; i++ {
			queryOp.Config.Shards[i] = oldConfig.Shards[i]
		}

		sm.px.Start(seq, queryOp)

		timeout := 10 * time.Millisecond
		for !sm.dead {
			dec, val := sm.px.Status(seq)
			if dec {
				sm.done = seq
				if val.(Op).Operation != "Query" {
					sm.configNum++
				}
				if val.(Op).Operation == queryOp.Operation && val.(Op).QueryID == queryOp.QueryID {
					// sm.configs = append(sm.configs, queryOp.Config)
					// sm.configs[sm.done] = moveOp.Config
					agreement = true
					break
				} else if val.(Op).Operation != "Query" {
					sm.configs = append(sm.configs, val.(Op).Config)
					break
				} else {
					break
				}
			}

			time.Sleep(timeout)
			if timeout < 10*time.Second {
				timeout *= 2
			}
		}
	}

	/*for {
		seq += 1
		decided, value := sm.px.Status(seq)
		fmt.Printf("Sequence number %d: %t, %v\n", seq, decided, value)
		if !decided {
			break
		} else {
			sm.configs = append(sm.configs, value.(Op).Config)
			sm.done = seq
			sm.px.Done(sm.done)
		}
	}*/

	// fmt.Printf("In Query(), finished updating, done is %d\n", sm.done)
	// fmt.Printf("In Query(), config is %v\n", sm.configs[sm.done])

	if args.Num == -1 {
		// fmt.Printf("config %d, had groups %v\n", args.Num, sm.configs[sm.done])
		// reply.Config = sm.configs[sm.done]
		reply.Config = sm.configs[sm.configNum]
	} else {
		// fmt.Printf("config %d, had groups %v\n", args.Num, sm.configs[args.Num])
		if len(sm.configs) > args.Num {
			reply.Config = sm.configs[args.Num]
		} else {
			reply.Config = Config{}
		}
	}

	return nil
}

func nrand() int64 {
	// max := big.NewInt(int64(1) << 62)
	bigx := rand.Int()
	x := int64(bigx)
	return x
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.done = 0
	sm.joined = make(map[int64]bool)
	sm.left = make(map[int64]bool)
	sm.workload = make(map[int64][]int64)
	sm.configNum = 0

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
