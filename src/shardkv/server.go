package shardkv

import (
	"net"
	"strconv"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Operation string
	RequestID string

	// for put/puthash
	PutKey  string
	Value   string
	PutHash bool

	// for get
	GetKey string

	// for put/get
	// ConfigNumWhenProcessed int

	// for reconfiguration
	Reconfig shardmaster.Config
}

type shardData struct {
	// Shard int64
	Data            map[string]string
	VisitedRequests map[string]string
}

type ShardKV struct {
	mu         sync.Mutex // protects globalSeq and ConfigMap
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	// dataByShard      map[int]*shardData         // maps a particular shard to its keys/values
	dataPerConfig map[int]map[int]*shardData // Maintains cache per previous configs of the shard data
	ConfigMap     map[int]shardmaster.Config
	// curr_config      shardmaster.Config         // current configuration
	globalSeq        int
	currentConfigNum int
	cacheMu          sync.Mutex // Protects dataPerConfig
	configNumMu      sync.Mutex // Protects currentConfigNum
	reconfiguring    bool
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	// time.Sleep(20 * time.Millisecond)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	/*if kv.reconfiguring {
		reply.Err = ErrWrongGroup
		return nil
	}*/

	kv.configNumMu.Lock()
	getOp := Op{
		Operation: "Get",
		GetKey:    args.Key,
		RequestID: args.RequestID,
		// ConfigNumWhenProcessed: kv.currentConfigNum,
	}
	kv.configNumMu.Unlock()

	err, value := kv.makeAgreementandApplyChange(getOp)
	reply.Err = err
	reply.Value = value

	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	// time.Sleep(20 * time.Millisecond)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	/*if kv.reconfiguring {
		reply.Err = ErrWrongGroup
		return nil
	}*/

	kv.configNumMu.Lock()
	putOp := Op{
		Operation: "Put",
		PutKey:    args.Key,
		Value:     args.Value,
		PutHash:   args.DoHash,
		RequestID: args.RequestID,
		// ConfigNumWhenProcessed: kv.currentConfigNum,
	}
	kv.configNumMu.Unlock()

	err, value := kv.makeAgreementandApplyChange(putOp)
	reply.Err = err
	reply.PreviousValue = value

	return nil
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	// time.Sleep(20 * time.Millisecond)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	/*if kv.reconfiguring {
		return
	}*/

	/*next_config := shardmaster.Config{}

	if kv.currentConfigNum == 0 {
		next_config = kv.sm.Query(0)
	} else {
		next_config = kv.sm.Query(kv.currentConfigNum + 1)
	}*/

	kv.configNumMu.Lock()
	next_config := kv.sm.Query(kv.currentConfigNum + 1)
	DPrintf("I am %v from gid %d, Calling tick(). Got latest config to be %v\n", kv.me, kv.gid, next_config)
	// DPrintf("I am %v, latest_config is %v, curr_config is %v\n", kv.me, latest_config, kv.curr_config)

	if next_config.Num != kv.currentConfigNum+1 {
		kv.configNumMu.Unlock()
		return
	}
	/*index := kv.currentConfigNum + 1
	for {
		config_index := kv.sm.Query(index)
		// empty_config := shardmaster.Config{}
		if config_index.Num != 0 {
			break
		}

		reconfigOp := Op{
			Operation:              "Reconfiguration",
			Reconfig:               config_index,
			RequestID:              generateID(),
			ConfigNumWhenProcessed: kv.currentConfigNum,
		}

		kv.makeAgreementandApplyChange(reconfigOp)
		kv.currentConfigNum = index

		index++
	}*/

	reconfigOp := Op{
		Operation: "Reconfiguration",
		Reconfig:  next_config,
		// RequestID:              generateID(),
		RequestID: strconv.Itoa(next_config.Num),
		// ConfigNumWhenProcessed: kv.currentConfigNum,
	}
	kv.configNumMu.Unlock()

	err, _ := kv.makeAgreementandApplyChange(reconfigOp)
	DPrintf("I am %d from gid %d. I tried to reconfigure on config num %d, err is %v", kv.me, kv.gid, next_config.Num, err)

	/*if err == OK {
		kv.configNumMu.Lock()
		DPrintf("I am %d from gid %d, Set current config num to %d", kv.me, kv.gid, next_config.Num)
		kv.currentConfigNum = next_config.Num
		kv.configNumMu.Unlock()
	}*/
}

func (kv *ShardKV) makeAgreementandApplyChange(op Op) (Err, string) {
	DPrintf("I am %d from gid %d, Starting to try for agreement on op %v. Global Seq is %d at time %v\n", kv.me, kv.gid, op, kv.globalSeq, time.Now())
	seq := kv.globalSeq + 1

	// Retry till this operation is entered into log.
	for {
		DPrintf("I am %v from gid %d is proposing seq %v with op %v\n", kv.me, kv.gid, seq, op)
		kv.px.Start(seq, op)
		agreedV := kv.WaitForAgreement(seq)
		if agreedV.(Op).RequestID != op.RequestID {
			seq++
			continue
		}
		DPrintf("I am %v from gid %d propose seq %v OK with op %v\n", kv.me, kv.gid, seq, op)
		// OK.
		break
	}

	/*if kv.reconfiguring {
		return ErrWrongGroup, ""
	}*/
	// If [globalSeq+1, seq) is not null, then there is some log
	// beyond our commit, catch up first:
	idx := kv.globalSeq + 1

	// for idx := kv.globalSeq + 1; idx < seq; idx++ {
	for idx < seq {
		v := kv.WaitForAgreement(idx)
		DPrintf("I am %v from gid %d, Applying change for index %d\n", kv.me, kv.gid, idx)
		kv.applyChange(v.(Op))
		idx++
	}
	// Now we can apply our op, and return the value
	DPrintf("I am %v from gid %d, Applying change for index %d\n", kv.me, kv.gid, seq)
	ret1, ret2 := kv.applyChange(op)

	// Update global seq
	kv.globalSeq = seq
	DPrintf("I am %d from gid %d, Setting globalSeq to %d\n", kv.me, kv.gid, kv.globalSeq)

	// mark seq as done.
	kv.px.Done(seq)
	return ret1, ret2
}

func (kv *ShardKV) WaitForAgreement(seq int) interface{} {
	for {
		ok, v := kv.px.Status(seq)
		if ok {
			return v
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// applyChange Applies the operation (Op) to the server database.
// the caller should hold the lock
func (kv *ShardKV) applyChange(op Op) (Err, string) {
	DPrintf("I am %v from gid %d, Trying to apply change for %v\n", kv.me, kv.gid, op)
	if op.Operation == "Put" {
		// Case 1: Put
		// cwp := op.ConfigNumWhenProcessed

		kv.configNumMu.Lock()
		ccn := kv.currentConfigNum
		kv.configNumMu.Unlock()

		shard := key2shard(op.PutKey)
		if kv.ConfigMap[ccn].Shards[shard] != kv.gid { // if I do not handle this shard based on the current config, then do not process the request
			return ErrWrongGroup, ""
		}
		// check if already applied?
		prevV := ""
		found := false

		kv.cacheMu.Lock()
		if kv.dataPerConfig[ccn][shard] == nil {
			found = false
		} else {
			prevV, found = kv.dataPerConfig[ccn][shard].VisitedRequests[op.RequestID]
		}
		kv.cacheMu.Unlock()

		// kv.dataByShard[shard].VisitedRequests[op.RequestID]
		if found {
			return OK, prevV
		}

		// get the old value and new value based on Hash/noHash.
		kv.cacheMu.Lock()
		DPrintf("ccn is %d, %v\n", ccn, kv.dataPerConfig[ccn])
		oldValue := ""
		if kv.dataPerConfig[ccn][shard] != nil {
			oldValue = kv.dataPerConfig[ccn][shard].Data[op.PutKey]
		}
		// oldValue := kv.dataPerConfig[ccn][shard].Data[op.PutKey]
		kv.cacheMu.Unlock()
		// kv.dataByShard[shard].Data[op.PutKey]
		newValue := ""

		if op.PutHash {
			newValue = strconv.Itoa(int(hash(oldValue + op.Value)))
		} else {
			newValue = op.Value
		}

		DPrintf("I am %v from gid %d, apply op: %v, putKey: %v, old: %v, new: %v, shard: %v\n", kv.me, kv.gid, op, op.PutKey, oldValue, newValue, shard)

		kv.cacheMu.Lock()
		DPrintf("%v\n", kv.dataPerConfig[ccn])
		kv.dataPerConfig[ccn][shard].Data[op.PutKey] = newValue
		kv.cacheMu.Unlock()

		// Only PutHash needs old value:
		if !op.PutHash {
			// Discard to save memory
			oldValue = ""
		}

		// update request reply DB.
		kv.cacheMu.Lock()
		kv.dataPerConfig[ccn][shard].VisitedRequests[op.RequestID] = oldValue
		kv.cacheMu.Unlock()

		return OK, oldValue

	} else if op.Operation == "Get" {
		// Case 2: Get, If key present in DB return value else return ErrNoKey.
		shard := key2shard(op.GetKey)

		kv.configNumMu.Lock()
		ccn := kv.currentConfigNum
		kv.configNumMu.Unlock()

		if kv.ConfigMap[ccn].Shards[shard] != kv.gid { // if I do not handle this shard based on the current config, then do not process the request
			return ErrWrongGroup, ""
		}

		kv.cacheMu.Lock()
		value, found := kv.dataPerConfig[ccn][shard].Data[op.GetKey]
		kv.cacheMu.Unlock()
		DPrintf("I am %v from gid %d, apply op: %v, getKey: %v, value: %v, shard: %v\n", kv.me, kv.gid, op, op.GetKey, value, shard)

		if found {
			return OK, value
		} else {
			return ErrNoKey, ""
		}
	} else if op.Operation == "Reconfiguration" {
		// Case 3: Reconfiguration. Will need to update configuration and get/send shard data around

		// TODO: Make RPC to move shard data around
		// find all shards that need to be pulled
		// for each shard that needs to be pulled, find the group responsible for it
		// try all servers within that group until we get the data we want

		DPrintf("I am %v from gid %d, reconfiguring for config number: %d\n", kv.me, kv.gid, op.Reconfig.Num)

		// deepCopyOfDBS := make(map[int]*shardData)
		kv.configNumMu.Lock()
		ccn := kv.currentConfigNum
		kv.configNumMu.Unlock()

		kv.cacheMu.Lock()
		kv.dataPerConfig[op.Reconfig.Num] = make(map[int]*shardData)

		for i := 0; i < shardmaster.NShards; i++ {
			origShardData := kv.dataPerConfig[ccn][i]
			kv.dataPerConfig[op.Reconfig.Num][i] = &shardData{
				Data:            make(map[string]string),
				VisitedRequests: make(map[string]string),
			}
			for k, v := range origShardData.Data {
				kv.dataPerConfig[op.Reconfig.Num][i].Data[k] = v
			}
			for k, v := range origShardData.VisitedRequests {
				kv.dataPerConfig[op.Reconfig.Num][i].VisitedRequests[k] = v
			}
		}

		// kv.dataPerConfig[op.Reconfig.Num] = deepCopyOfDBS // Moving to new configuration, so copy the current data over
		DPrintf("I am %v from gid %d, Moved data over to %d. ccn is %d, Data is %v\n", kv.me, kv.gid, op.Reconfig.Num, ccn, kv.dataPerConfig[ccn])
		kv.cacheMu.Unlock()

		newRes := newResponsibilities(kv.gid, kv.ConfigMap[ccn], op.Reconfig)
		DPrintf("Computed New Responsibilites as %v\n", newRes)

		// TODO: set reconfig flag
		/*kv.reconfiguring = true
		DPrintf("I am %d from gid %d, I set reconfiguring to true", kv.me, kv.gid)
		kv.mu.Unlock()*/

		for _, shard := range newRes {
			group := kv.ConfigMap[ccn].Shards[shard]
			gotData := false
			for !gotData && !kv.dead {
				for _, server := range kv.ConfigMap[ccn].Groups[group] {
					pullArg := PullArgs{
						ConfigNum: ccn,
						ShardNum:  shard,
					}

					pullReply := PullReply{}
					c1 := make(chan bool, 1)
					DPrintf("I am %v from gid %d, making call to %v\n", kv.me, kv.gid, server)
					go func() {
						c1 <- call(server, "ShardKV.PullRequest", &pullArg, &pullReply)
					}()

					ok := false
					select {
					case res := <-c1:
						ok = res
					case <-time.After(2 * time.Second):
						DPrintf("Timeout\n")
						continue
					}
					DPrintf("I am %v from gid %d, and call completed and ok is %v\n", kv.me, kv.gid, ok)

					if ok && pullReply.Err == OK {
						// kv.dataByShard[shard] = pullReply.ShardData // Deep copy maybe?

						DPrintf("I am %d from gid %d, and I received %v for shard %d\n", kv.me, kv.gid, pullReply.ShardData.Data, shard)
						kv.cacheMu.Lock()
						for k, v := range pullReply.ShardData.Data {
							kv.dataPerConfig[op.Reconfig.Num][shard].Data[k] = v
						}
						for k, v := range pullReply.ShardData.VisitedRequests {
							kv.dataPerConfig[op.Reconfig.Num][shard].VisitedRequests[k] = v
						}
						kv.cacheMu.Unlock()
						gotData = true
						break
					}
				}
			}
		}
		/*kv.curr_config.Num = op.Config.Num
		for i := 0; i < len(op.Config.Shards); i++ {
			kv.curr_config.Shards[i] = op.Config.Shards[i]
		}
		for k, v := range op.Config.Groups {
			kv.curr_config.Groups[k] = v
		}*/
		newestConfig := shardmaster.Config{}
		newestConfig.Num = op.Reconfig.Num
		newestConfig.Groups = make(map[int64][]string)

		for i := 0; i < len(op.Reconfig.Shards); i++ {
			newestConfig.Shards[i] = op.Reconfig.Shards[i]
		}
		for k, v := range op.Reconfig.Groups {
			newestConfig.Groups[k] = v
		}

		//kv.mu.Lock()
		//DPrintf("I am %d from gid %d, I set reconfiguring to false", kv.me, kv.gid)
		kv.ConfigMap[op.Reconfig.Num] = newestConfig
		// kv.reconfiguring = false
		// TODO: unset the reconfig flag

		// kv.curr_config = op.Config
		// once we have all the data we need, set the curr_config to the updated one
		kv.configNumMu.Lock()
		kv.currentConfigNum = op.Reconfig.Num
		kv.configNumMu.Unlock()

		return OK, ""
	} else {
		// Invalid Operation
		return "", ""
	}
}

func newResponsibilities(gid int64, oldConfig shardmaster.Config, newConfig shardmaster.Config) []int {
	newRes := make([]int, 0)

	for i := 0; i < shardmaster.NShards; i++ {
		if newConfig.Shards[i] == gid && oldConfig.Shards[i] != gid && oldConfig.Shards[i] != 0 {
			newRes = append(newRes, i)
		}
	}
	return newRes
}

func (kv *ShardKV) PullRequest(args *PullArgs, reply *PullReply) error {

	kv.configNumMu.Lock()
	if kv.currentConfigNum < args.ConfigNum+1 {
		kv.configNumMu.Unlock()
		reply.Err = ErrNotCaughtUp
		// kv.tick() // Questionable....
		return nil
	}
	kv.configNumMu.Unlock()

	kv.cacheMu.Lock()
	shard_data := kv.dataPerConfig[args.ConfigNum][args.ShardNum]
	DPrintf("I am %d from gid %d, I am sending over %v for shard %d and configNum %d\n", kv.me, kv.gid, kv.dataPerConfig[args.ConfigNum][args.ShardNum].Data, args.ShardNum, args.ConfigNum)
	kv.cacheMu.Unlock()
	// kv.data_history[args.ConfigNum][args.ShardNum]
	reply.ShardData = shard_data
	reply.Err = OK

	return nil
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().

	kv.dataPerConfig = make(map[int]map[int]*shardData)
	kv.ConfigMap = make(map[int]shardmaster.Config)
	kv.currentConfigNum = 0
	kv.globalSeq = 0
	DPrintf("Starting the server %d from gid %d, setting global_seq to %d\n", kv.me, kv.gid, kv.globalSeq)

	kv.ConfigMap[0] = shardmaster.Config{}
	kv.dataPerConfig[0] = make(map[int]*shardData)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.dataPerConfig[0][i] = &shardData{
			Data:            make(map[string]string),
			VisitedRequests: make(map[string]string),
		}
	}
	kv.reconfiguring = false
	DPrintf("kv.dataPerconfig[0] is %v\n", kv.dataPerConfig[0])

	/*kv.dataByShard = make(map[int]*shardData) // maps a particular shard to its keys/values
	for i := 0; i < shardmaster.NShards; i++ {
		kv.dataByShard[i] = &shardData{
			Data:            make(map[string]string),
			VisitedRequests: make(map[string]string),
		}
		kv.dataByShard[i].Data = make(map[string]string)
		kv.dataByShard[i].VisitedRequests = make(map[string]string)
	}
	kv.data_history = make(map[int]map[int]*shardData) // Maintains cache per previous configs of the shard data
	kv.curr_config = shardmaster.Config{}
	kv.curr_config.Groups = make(map[int64][]string)*/

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
