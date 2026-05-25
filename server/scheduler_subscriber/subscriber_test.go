package schedulersubscriber

import (
	"testing"

	"github.com/wk-y/rama-swap/microservices/scheduling"
	"github.com/wk-y/rama-swap/tracker"
)

type recordingScheduler struct {
	connects    []scheduling.Node
	disconnects []scheduling.Node
}

func (r *recordingScheduler) OnNewTask(task scheduling.Task)       {}
func (r *recordingScheduler) OnTaskCancelled(task scheduling.Task) {}
func (r *recordingScheduler) OnNodeConnect(node scheduling.Node) {
	r.connects = append(r.connects, node)
}
func (r *recordingScheduler) OnNodeDisconnect(node scheduling.Node) {
	r.disconnects = append(r.disconnects, node)
}

func TestOnNodeUpdatedDoesNotReconnectForMaxSizeOnlyChange(t *testing.T) {
	scheduler := &recordingScheduler{}
	subscriber := NewSchedulerSubscriber(scheduler)

	initial := tracker.RpcServerInfo{
		Id:      "node-1",
		Ip:      "192.168.1.2",
		Port:    1234,
		MaxSize: 100,
	}
	subscriber.OnNodeAdded(initial)

	updated := initial
	updated.MaxSize = 250
	subscriber.OnNodeUpdated(updated)

	if len(scheduler.connects) != 1 {
		t.Fatalf("connect calls = %d, want 1", len(scheduler.connects))
	}
	if len(scheduler.disconnects) != 0 {
		t.Fatalf("disconnect calls = %d, want 0", len(scheduler.disconnects))
	}

	stored := subscriber.nodes[initial.Id]
	if got := stored.MaxSize(); got != 250 {
		t.Fatalf("stored MaxSize = %d, want 250", got)
	}
}

func TestOnNodeUpdatedReconnectsForAddressChange(t *testing.T) {
	scheduler := &recordingScheduler{}
	subscriber := NewSchedulerSubscriber(scheduler)

	initial := tracker.RpcServerInfo{
		Id:      "node-1",
		Ip:      "192.168.1.2",
		Port:    1234,
		MaxSize: 100,
	}
	subscriber.OnNodeAdded(initial)

	updated := initial
	updated.Port = 4321
	subscriber.OnNodeUpdated(updated)

	if len(scheduler.connects) != 2 {
		t.Fatalf("connect calls = %d, want 2", len(scheduler.connects))
	}
	if len(scheduler.disconnects) != 1 {
		t.Fatalf("disconnect calls = %d, want 1", len(scheduler.disconnects))
	}
}
