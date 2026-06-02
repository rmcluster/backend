package scheduling

import "testing"

func TestPartitioningSchedulerTunablesFromConstructor(t *testing.T) {
	scheduler := NewPartitioningScheduler(nil, 3)
	if got := scheduler.getParallelismTarget(); got != 3 {
		t.Fatalf("getParallelismTarget() = %d, want 3", got)
	}
}
