package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		select {
		case <-time.After(time.Second):
			done := doHeartBeat()
			if done {
				return
			}
		default:
			go reqTaskAndWork(mapf, reducef)
			time.Sleep(time.Second)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doHeartBeat() bool {
	reply := Receipt{}
	ok := call("Coordinator.HeartBeat", &Signal{}, &reply)
	if ok {
		return reply.done
	} else {
		log.Fatalf("call Coordinator.HeatBeat failed")
		return false
	}
}

func reqTaskAndWork(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	task := Task{}
	ok := call("Coordinator.ReqTask", &Signal{}, &task)
	if ok {
		// Work on the task.
		switch task.t {
		case Map:
			// Apply mapf to file data.
			id, filename := task.id, task.file
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			// Disperse kva to n intermidate files.
			kvmap := make(map[int][]KeyValue)
			for i := 0; i < task.nReduce; i++ {
				kvmap[i] = make([]KeyValue, 0)
			}
			for _, kv := range kva {
				kvmap[id] = append(kvmap[ihash(kv.Key)%task.nReduce], kv)
			}
			for i := 0; i < task.nReduce; i++ {
				file, err = os.CreateTemp(".", "*")
				if err != nil {
					log.Fatalf("cannot create tmp file")
				}
				enc := json.NewEncoder(file)
				for _, kv := range kvmap[i] {
					enc.Encode(&kv)
				}
				os.Rename(file.Name(), fmt.Sprintf("mr-%v-%v", id, i))
			}
		case Reduce:
			// todo
		}
		// Inform Coordinator this work is done.
		// todo
	} else {
		log.Fatalf("call Coordinator.ReqMapTask failed")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
