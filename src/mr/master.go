package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


type Master struct {
	mu sync.Mutex

	files   []string
	nReduce int

	phase TaskType

	mapTaskStatus    []int
	mapTaskAttempts  []int
	mapTaskStartTime []time.Time

	reduceTaskStatus    []int
	reduceTaskAttempts  []int
	reduceTaskStartTime []time.Time
}

// Your code here -- RPC handlers for the worker to call.


func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.advancePhase()

	switch m.phase {
	case MapTask:
		for i := range m.mapTaskStatus {
			if m.mapTaskStatus[i] == Idle {
				m.assignMapTask(i, reply)
				return nil
			}
		}

		for i := range m.mapTaskStatus {
			if m.mapTaskStatus[i] == InProgress &&
				time.Since(m.mapTaskStartTime[i]) >= 10*time.Second {
				m.assignMapTask(i, reply)
				return nil
			}
		}

		reply.TaskType = WaitTask

	case ReduceTask:
		for i := range m.reduceTaskStatus {
			if m.reduceTaskStatus[i] == Idle {
				m.assignReduceTask(i, reply)
				return nil
			}
		}

		for i := range m.reduceTaskStatus {
			if m.reduceTaskStatus[i] == InProgress &&
				time.Since(m.reduceTaskStartTime[i]) >= 10*time.Second {
				m.assignReduceTask(i, reply)
				return nil
			}
		}

		reply.TaskType = WaitTask

	case ExitTask:
		reply.TaskType = ExitTask

	default:
		reply.TaskType = WaitTask
	}

	return nil
}

func (m *Master) assignMapTask(taskID int, reply *RequestTaskReply) {
	m.mapTaskStatus[taskID] = InProgress
	m.mapTaskAttempts[taskID]++
	m.mapTaskStartTime[taskID] = time.Now()

	reply.TaskType = MapTask
	reply.TaskID = taskID
	reply.Attempt = m.mapTaskAttempts[taskID]
	reply.Filename = m.files[taskID]
	reply.NReduce = m.nReduce
	reply.NMap = len(m.files)
}

func (m *Master) assignReduceTask(taskID int, reply *RequestTaskReply) {
	m.reduceTaskStatus[taskID] = InProgress
	m.reduceTaskAttempts[taskID]++
	m.reduceTaskStartTime[taskID] = time.Now()

	reply.TaskType = ReduceTask
	reply.TaskID = taskID
	reply.Attempt = m.reduceTaskAttempts[taskID]
	reply.NReduce = m.nReduce
	reply.NMap = len(m.files)
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		if args.TaskID >= 0 && args.TaskID < len(m.mapTaskStatus) {
			if m.phase == MapTask &&
				m.mapTaskStatus[args.TaskID] == InProgress &&
				m.mapTaskAttempts[args.TaskID] == args.Attempt {
				m.mapTaskStatus[args.TaskID] = Completed
			}
		}

	case ReduceTask:
		if args.TaskID >= 0 && args.TaskID < len(m.reduceTaskStatus) {
			if m.phase == ReduceTask &&
				m.reduceTaskStatus[args.TaskID] == InProgress &&
				m.reduceTaskAttempts[args.TaskID] == args.Attempt {
				m.reduceTaskStatus[args.TaskID] = Completed
			}
		}
	}

	m.advancePhase()

	return nil
}

func (m *Master) advancePhase() {
	if m.phase == MapTask {
		allCompleted := true

		for _, status := range m.mapTaskStatus {
			if status != Completed {
				allCompleted = false
				break
			}
		}

		if allCompleted {
			m.phase = ReduceTask
		}
	}

	if m.phase == ReduceTask {
		allCompleted := true

		for _, status := range m.reduceTaskStatus {
			if status != Completed {
				allCompleted = false
				break
			}
		}

		if allCompleted {
			m.phase = ExitTask
		}
	}
}


//
// an example RPC handler.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.advancePhase()

	return m.phase == ExitTask
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.files = files
	m.nReduce = nReduce
	m.phase = MapTask

	m.mapTaskStatus = make([]int, len(files))
	m.mapTaskAttempts = make([]int, len(files))
	m.mapTaskStartTime = make([]time.Time, len(files))

	m.reduceTaskStatus = make([]int, nReduce)
	m.reduceTaskAttempts = make([]int, nReduce)
	m.reduceTaskStartTime = make([]time.Time, nReduce)

	m.server()

	return &m
}
