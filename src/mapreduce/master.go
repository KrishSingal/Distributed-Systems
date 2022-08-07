package mapreduce

import (
	"container/list"
	"fmt"
	"log"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	ack := make(chan bool)
	mapJobIndex := 0
	reduceJobIndex := 0

	log.Printf("nMap %d", mr.nMap)
	log.Printf("nReduce %d", mr.nReduce)

	for mapJobIndex < mr.nMap {
		log.Printf("hi")
		go func(mapJobIndex int) {
			log.Printf("hi2")
			worker_str := <-mr.registerChannel

			args := &DoJobArgs{mr.file,
				"Map",
				mapJobIndex,
				mr.nReduce}
			var reply DoJobReply

			call(worker_str, "Worker.DoJob", args, &reply)

			for !reply.OK {
				// mr.registerChannel <- worker_str
				log.Printf("Failed, looking for new worker for job %d", mapJobIndex)
				worker_str = <-mr.registerChannel

				call(worker_str, "Worker.DoJob", args, &reply)
			}

			log.Printf("Finished job %d", mapJobIndex)
			ack <- true
			mr.registerChannel <- worker_str
		}(mapJobIndex)
		log.Printf("hi3")
		mapJobIndex++
	}

	for i := 0; i < mr.nMap; i++ {
		// log.Printf("ack%d", i)
		<-ack
	}

	/*if len(ack) != mr.nMap {
		log.Fatal("map: ", len(ack))
	}*/

	// Reduce

	r_ack := make(chan bool)
	for reduceJobIndex < mr.nReduce {
		go func(reduceJobIndex int) {
			worker_str := <-mr.registerChannel

			args := &DoJobArgs{mr.file,
				"Reduce",
				reduceJobIndex,
				mr.nMap}
			var reply DoJobReply

			call(worker_str, "Worker.DoJob", args, &reply)

			for !reply.OK {
				worker_str = <-mr.registerChannel

				call(worker_str, "Worker.DoJob", args, &reply)
			}

			r_ack <- true
			mr.registerChannel <- worker_str
		}(reduceJobIndex)
		reduceJobIndex++
	}

	for i := 0; i < mr.nReduce; i++ {
		// log.Printf("r_ack%d", i)
		<-r_ack
	}

	return mr.KillWorkers()
}
