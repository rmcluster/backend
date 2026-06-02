package scheduling

import (
	"fmt"
	"math"
)

const (
	TunableParallelismTarget      = "parallelism_target"
	TunableMemoryTargetMultiplier = "memory_target_multiplier"
)

type TunableKind string

const (
	TunableKindInt   TunableKind = "int"
	TunableKindFloat TunableKind = "float"
)

type TunableSpec struct {
	Key         string      `json:"key"`
	Label       string      `json:"label"`
	Description string      `json:"description,omitempty"`
	Kind        TunableKind `json:"kind"`
	Unit        string      `json:"unit,omitempty"`
	Min         *float64    `json:"min,omitempty"`
	Max         *float64    `json:"max,omitempty"`
}

// TunableScheduler exposes scheduler-specific runtime parameters for the UI.
type TunableScheduler interface {
	Scheduler
	SchedulerName() string
	TunableSpecs() []TunableSpec
	TunableValues() map[string]any
	ApplyTunables(values map[string]any) error
}

func NumberPtr(v float64) *float64 {
	return &v
}

func ParseTunableInt(values map[string]any, key string) (int, error) {
	raw, ok := values[key]
	if !ok {
		return 0, fmt.Errorf("missing tunable %q", key)
	}
	switch v := raw.(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case float64:
		if math.Trunc(v) != v {
			return 0, fmt.Errorf("tunable %q must be a whole number", key)
		}
		return int(v), nil
	default:
		return 0, fmt.Errorf("tunable %q has invalid type %T", key, raw)
	}
}

func ParseTunableFloat(values map[string]any, key string) (float64, error) {
	raw, ok := values[key]
	if !ok {
		return 0, fmt.Errorf("missing tunable %q", key)
	}
	switch v := raw.(type) {
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("tunable %q has invalid type %T", key, raw)
	}
}

func ValidateTunableInt(spec TunableSpec, value int) error {
	if spec.Kind != TunableKindInt {
		return fmt.Errorf("tunable %q is not an integer", spec.Key)
	}
	if spec.Min != nil && float64(value) < *spec.Min {
		return fmt.Errorf("tunable %q must be >= %g", spec.Key, *spec.Min)
	}
	if spec.Max != nil && float64(value) > *spec.Max {
		return fmt.Errorf("tunable %q must be <= %g", spec.Key, *spec.Max)
	}
	return nil
}

func ValidateTunableFloat(spec TunableSpec, value float64) error {
	if spec.Kind != TunableKindFloat {
		return fmt.Errorf("tunable %q is not a float", spec.Key)
	}
	if spec.Min != nil && value < *spec.Min {
		return fmt.Errorf("tunable %q must be >= %g", spec.Key, *spec.Min)
	}
	if spec.Max != nil && value > *spec.Max {
		return fmt.Errorf("tunable %q must be <= %g", spec.Key, *spec.Max)
	}
	return nil
}

func floatPtr(v float64) *float64 {
	return NumberPtr(v)
}

func tunableInt(values map[string]any, key string) (int, error) {
	return ParseTunableInt(values, key)
}

func tunableFloat(values map[string]any, key string) (float64, error) {
	return ParseTunableFloat(values, key)
}

func validateInt(spec TunableSpec, value int) error {
	return ValidateTunableInt(spec, value)
}

func validateFloat(spec TunableSpec, value float64) error {
	return ValidateTunableFloat(spec, value)
}
