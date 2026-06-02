package scheduling

import (
	"testing"

	"github.com/rmcluster/backend/llama"
)

func TestEventDrivenSchedulerTunables(t *testing.T) {
	scheduler := NewEventDrivenScheduler(nil, llama.Llama{}, 1.0)

	values := scheduler.TunableValues()
	if values[TunableMemoryTargetMultiplier] != 1.0 {
		t.Fatalf("default multiplier = %v, want 1", values[TunableMemoryTargetMultiplier])
	}

	if err := scheduler.ApplyTunables(map[string]any{
		TunableMemoryTargetMultiplier: 1.5,
	}); err != nil {
		t.Fatalf("ApplyTunables: %v", err)
	}

	values = scheduler.TunableValues()
	if values[TunableMemoryTargetMultiplier] != 1.5 {
		t.Fatalf("multiplier after set = %v, want 1.5", values[TunableMemoryTargetMultiplier])
	}
}

func TestPartitioningSchedulerTunables(t *testing.T) {
	scheduler := NewPartitioningScheduler(nil, 3)

	if err := scheduler.ApplyTunables(map[string]any{TunableParallelismTarget: 6}); err != nil {
		t.Fatalf("ApplyTunables: %v", err)
	}
	if scheduler.getParallelismTarget() != 6 {
		t.Fatalf("parallelism = %d, want 6", scheduler.getParallelismTarget())
	}

	if err := scheduler.ApplyTunables(map[string]any{TunableParallelismTarget: 0}); err == nil {
		t.Fatal("expected validation error for parallelism_target < 1")
	}
}
