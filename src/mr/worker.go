package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "io"
import "os"
import "sort"
import "time"


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

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}

		ok := call("Master.RequestTask", &args, &reply)
		if !ok {
			return
		}

		switch reply.TaskType {

		case MapTask:
			err := doMap(reply, mapf)
			if err != nil {
				log.Printf("map task failed: %v", err)
				return
			}

			report := ReportTaskArgs{
				TaskType: MapTask,
				TaskID:   reply.TaskID,
				Attempt:  reply.Attempt,
			}

			result := ReportTaskReply{}
			if !call("Master.ReportTask", &report, &result) {
				return
			}

		case ReduceTask:
			err := doReduce(reply, reducef)
			if err != nil {
				log.Printf("reduce task failed: %v", err)
				return
			}

			report := ReportTaskArgs{
				TaskType: ReduceTask,
				TaskID:   reply.TaskID,
				Attempt:  reply.Attempt,
			}

			result := ReportTaskReply{}
			if !call("Master.ReportTask", &report, &result) {
				return
			}

		case WaitTask:
			time.Sleep(time.Second)

		case ExitTask:
			return

		default:
			time.Sleep(time.Second)
		}
	}
}

func doMap(
	task RequestTaskReply,
	mapf func(string, string) []KeyValue,
) error {

	data, err := os.ReadFile(task.Filename)
	if err != nil {
		return err
	}

	kva := mapf(task.Filename, string(data))

	if task.NReduce <= 0 {
		return fmt.Errorf("invalid number of reduce tasks")
	}

	files := make([]*os.File, task.NReduce)
	encoders := make([]*json.Encoder, task.NReduce)

	for i := 0; i < task.NReduce; i++ {
		file, err := os.CreateTemp(".", "mr-map-*")
		if err != nil {
			return err
		}

		files[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	for _, kv := range kva {
		reduceID := ihash(kv.Key) % task.NReduce

		err := encoders[reduceID].Encode(&kv)
		if err != nil {
			return err
		}
	}

	for i, file := range files {
		err := file.Close()
		if err != nil {
			return err
		}

		finalName := fmt.Sprintf(
			"mr-%d-%d",
			task.TaskID,
			i,
		)

		err = os.Rename(file.Name(), finalName)
		if err != nil {
			return err
		}
	}

	return nil
}

func doReduce(
	task RequestTaskReply,
	reducef func(string, []string) string,
) error {

	groups := make(map[string][]string)

	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf(
			"mr-%d-%d",
			i,
			task.TaskID,
		)

		file, err := os.Open(filename)
		if err != nil {
			return err
		}

		decoder := json.NewDecoder(file)

		for {
			var kv KeyValue

			err := decoder.Decode(&kv)

			if err == io.EOF {
				break
			}

			if err != nil {
				file.Close()
				return err
			}

			groups[kv.Key] = append(
				groups[kv.Key],
				kv.Value,
			)
		}

		if err := file.Close(); err != nil {
			return err
		}
	}

	keys := make([]string, 0, len(groups))

	for key := range groups {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	output, err := os.CreateTemp(".", "mr-reduce-*")
	if err != nil {
		return err
	}

	for _, key := range keys {
		value := reducef(key, groups[key])

		_, err := fmt.Fprintf(
			output,
			"%v %v\n",
			key,
			value,
		)

		if err != nil {
			output.Close()
			return err
		}
	}

	if err := output.Close(); err != nil {
		return err
	}

	finalName := fmt.Sprintf(
		"mr-out-%d",
		task.TaskID,
	)

	return os.Rename(output.Name(), finalName)
}

//
// example function to show how to make an RPC call to the master.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
