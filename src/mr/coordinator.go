package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu   sync.RWMutex
	done bool

	ReqCh  chan struct{}
	RespCh chan Task
	DoneCh chan Task
}

type Signal struct{}
type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type Task struct {
	id      int
	t       TaskType
	nReduce int
	file    string
}
type Receipt struct {
	done bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReqTask(args *Signal, reply Task) error {
	c.ReqCh <- struct{}{}
	reply = <-c.RespCh
	return nil
}

func (c *Coordinator) SummitDone(args *Task, reply *Signal) error {
	c.DoneCh <- *args
	return nil
}

func (c *Coordinator) HeartBeat(args *Signal, reply *Receipt) error {
	c.mu.RLock()
	reply.done = c.done
	c.mu.RUnlock()
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
	c.mu.RLock()
	ret = c.done
	c.mu.RUnlock()

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
	c.ReqCh = make(chan struct{})
	c.RespCh = make(chan Task)
	c.DoneCh = make(chan Task)
	go c.coordinate(files, nReduce)

	c.server()
	return &c
}

//
// change states of the Coordinator.
// All modification of fields in the Coordinator happen here,
// this excludes the use of locks.
//
func (c *Coordinator) coordinate(files []string, nReduce int) {
	taskType, nMap := Map, len(files)
	issued, timeoutCh, liveChs := getIndicators(nMap)
	for {
		select {
		case <-c.ReqCh:
			for id := range issued {
				if !issued[id] {
					issued[id] = true
					task := Task{id: id, t: taskType}
					if taskType == Map {
						task.file = files[id]
						task.nReduce = nReduce
					}
					c.RespCh <- task
					go func() {
						select {
						case <-time.After(10 * time.Second):
							timeoutCh <- id
						case <-liveChs[id]:
						}
					}()
					break
				}
			}
		case id := <-timeoutCh:
			issued[id] = false
		case task := <-c.DoneCh:
			liveChs[task.id] <- struct{}{}
			delete(issued, task.id)
			if len(issued) == 0 {
				switch taskType {
				case Map:
					taskType = Reduce
					issued, timeoutCh, liveChs = getIndicators(nReduce)
				case Reduce:
					c.mu.Lock()
					c.done = true
					c.mu.Unlock()
				}
			}
		}
	}
}

func getIndicators(n int) (map[int]bool, chan int, map[int]chan struct{}) {
	issued, timeoutCh, liveChs := make(map[int]bool, n), make(chan int, n), make(map[int]chan struct{}, n)
	for id := 0; id < n; id++ {
		issued[id] = false
		liveChs[id] = make(chan struct{})
	}
	return issued, timeoutCh, liveChs
}
