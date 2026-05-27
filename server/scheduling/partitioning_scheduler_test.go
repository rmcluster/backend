package scheduling

import "testing"

func TestPartitioningSchedulerParallelismTarget(t *testing.T) {
	scheduler := NewPartitioningScheduler(nil, 3)

	if got := scheduler.GetParallelismTarget(); got != 3 {
		t.Fatalf("GetParallelismTarget() = %d, want 3", got)
	}

	scheduler.SetParallelismTarget(6)
	if got := scheduler.GetParallelismTarget(); got != 6 {
		t.Fatalf("GetParallelismTarget() after set = %d, want 6", got)
	}

	scheduler.SetParallelismTarget(0)
	if got := scheduler.GetParallelismTarget(); got != 1 {
		t.Fatalf("GetParallelismTarget() after clamped set = %d, want 1", got)
	}
}
