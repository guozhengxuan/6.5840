package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	files map[int]string
	nReduce int

	issued map[int]bool
	mapReqCh chan struct{}
	mapRespCh chan int
	mapDoneCh chan int

	reduceReadyCh chan bool
	reduceReqCh chan struct{}
	reduceRespCh chan int
	reduceDoneCh chan int

	finishCh chan bool
}

type Signal struct {}

type Task struct {
	id int
	nReduce int
	file string
}

type ReduceTask struct {
	id int
}

type TasksAllDone struct { isDone bool }

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReqMapTask(args *Signal, reply *Task) error {
	c.mapReqCh <- struct{}{}
	id := <-c.mapRespCh
	reply.id = id
	reply.file = c.files[id]
	reply.nReduce = c.nReduce
	return nil
}

func (c *Coordinator) MarkMapDone(args *Task, reply *TasksAllDone) error {
	c.mapDoneCh <- args.id
	reply.isDone = <-c.reduceReadyCh
	return nil
}

func (c *Coordinator) ReqReduceTask(args *Signal, reply *ReduceTask) error {
	c.reduceReqCh <- struct{}{}
	id := <-c.reduceRespCh
	reply.id = id
	return nil
}

func (c *Coordinator) MarkReduceDone(args *ReduceTask, reply *TasksAllDone) error {
	c.reduceDoneCh <- args.id
	reply.isDone = <-c.finishCh
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// Init coordinator.
	for i, file := range files {
		c.files[i] = file
	}
	c.nReduce = nReduce
	c.issued = make(map[int]bool, len(files))
	for id := range c.files {
		c.issued[id] = false
	}
	c.mapReqCh = make(chan struct{})
	c.mapRespCh = make(chan int)
	c.mapDoneCh = make(chan int)
	c.reduceReadyCh = make(chan bool)
	go c.coordinate()

	c.server()
	return &c
}

//
// change states of the Coordinator.
// All modification of fields in the Coordinator happen here,
// this excludes the use of locks.
//
func (c *Coordinator) coordinate() {
	doneChan := make(map[int]chan struct{}, len(c.files))
	for id := range c.files {
		doneChan[id] = make(chan struct{})
	}
	for {
		select {
		case <-c.mapReqCh:
			for id := range c.issued {
				if !c.issued[id] {
					c.issued[id] = true
					c.mapRespCh <- id
					go func () {
						select {
						case <-time.After(10 * time.Second):
							c.issued[id] = false
						case <-doneChan[id]:
						}
					}()
					break
				}
			}
		case id := <-c.mapDoneCh:
			doneChan[id] <- struct{}{}
			delete(c.issued, id)
			c.reduceReadyCh <- len(c.issued) == 0
		case <-c.reduceReqCh:

		}
	}
}