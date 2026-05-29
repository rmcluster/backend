package scheduling

// events that can be sent to the scheduler:
/*
Unexpected instance death/failure
Instance termination
Task completion (possibly making instance idle)
Queued task cancellation (client disconnects)
New task arrives
node connects
node disconnects
*/

type Scheduler interface {
	OnNewTask(Task)
	OnTaskCancelled(Task)
	OnNodeConnect(Node)
	OnNodeDisconnect(Node)
}
