
package mr

//
// RPC definitions.
//

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	TaskType TaskType

	TaskID  int
	Attempt int

	Filename string
	NReduce  int

	NMap int
}

type ReportTaskArgs struct {
	TaskType TaskType
	TaskID   int
	Attempt  int
}

type ReportTaskReply struct {
}
