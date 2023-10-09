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
	files []string
	nReduce int

	issued map[string]bool
	mapReqChan chan struct{}
	mapRespChan chan string
	mapDoneChan chan string
}

type Signal struct {}

type Task struct {
	file string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReqMapTask(args *Signal, reply *Task) error {
	c.mapReqChan <- struct{}{}
	file := <-c.mapRespChan
	reply.file = file
	return nil
}

func (c *Coordinator) MarkMapDone(args *Task, reply *Signal) error {
	c.mapDoneChan <- args.file
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
	c.files = files
	c.nReduce = nReduce
	c.issued = make(map[string]bool, len(files))
	for _, f := range files {
		c.issued[f] = false
	}
	c.mapReqChan = make(chan struct{})
	c.mapRespChan = make(chan string)
	c.mapDoneChan = make(chan string)
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
	doneChan := make(map[string]chan struct{}, len(c.files))
	for _, f := range c.files {
		doneChan[f] = make(chan struct{})
	}
	for {
		select {
		case <-c.mapReqChan:
			for f := range c.issued {
				if !c.issued[f] {
					c.issued[f] = true
					c.mapRespChan <- f
					go func ()  {
						select {
						case <-doneChan[f]:
						case <-time.After(10 * time.Second):
							c.issued[f] = false
						}
					}()
					break
				}
			}
		case f := <-c.mapDoneChan:
			delete(c.issued, f)
		}
	}
}