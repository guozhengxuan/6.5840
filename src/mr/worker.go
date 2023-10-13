package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
			reqTaskAndWork(mapf, reducef)
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
		return reply.Done
	} else {
		log.Println("call Coordinator.HeatBeat failed, assume coordinator exited")
		log.Println("exited...")
		return true
	}
}

func reqTaskAndWork(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {

	task := Task{}

	ok := call("Coordinator.ReqTask", &Signal{}, &task)
	if ok {
		if task == (Task{}) {
			return
		}

		// Work on a task.
		switch task.Type {
		case Map:
			// Apply mapf to file data.
			id, filename := task.Id, task.Fn

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}

			file.Close()

			// Collect kv pairs as map result.
			kva := mapf(filename, string(content))

			// Disperse kv pairs to n intermediate files.
			kvmap := make(map[int][]KeyValue)
			for i := 0; i < task.N; i++ {
				kvmap[i] = make([]KeyValue, 0)
			}
			for _, kv := range kva {
				i := ihash(kv.Key) % task.N
				kvmap[i] = append(kvmap[i], kv)
			}

			for i := 0; i < task.N; i++ {
				file, err = os.CreateTemp(".", "*")

				if err != nil {
					log.Fatalf("cannot create tmp file")
				}

				enc := json.NewEncoder(file)
				for _, kv := range kvmap[i] {
					enc.Encode(&kv)
				}

				file.Close()

				os.Rename(file.Name(), fmt.Sprintf("mr-%v-%v", id, i))
			}
		case Reduce:
			// Find all intermediate files of reduce task with id.
			files, err := filepath.Glob(fmt.Sprintf("mr-*-%v", task.Id))
			if err != nil {
				log.Fatalf("Error finding intermidiate files of reduce task [id: %v]", task.Id)
			}
			intermediate := make([]KeyValue, 0)

			// Read intermediate files.
			for _, fn := range files {
				file, err := os.Open(fn)
				if err != nil {
					log.Fatalf("Cannot open %v", fn)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			// Sort kvs and apply reducef on them.
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%v", task.Id)
			ofile, _ := os.CreateTemp(".", "*")

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}

				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}

				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()

			os.Rename(ofile.Name(), oname)
		}

		// Inform Coordinator this work is done.
		ok := call("Coordinator.SummitDone", &task, &Signal{})
		if !ok {
			log.Printf("call Coordinator.SummitDone failed")
		}
	} else {
		log.Printf("call Coordinator.ReqTask failed")
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
