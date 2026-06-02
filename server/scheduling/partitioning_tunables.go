package scheduling

import "fmt"

var partitioningTunableSpecs = []TunableSpec{
	{
		Key:         TunableParallelismTarget,
		Label:       "Parallelism target",
		Description: "Nodes to allocate per instance.",
		Kind:        TunableKindInt,
		Min:         floatPtr(1),
	},
}

func (s *PartitioningScheduler) SchedulerName() string {
	return "partitioning"
}

func (s *PartitioningScheduler) TunableSpecs() []TunableSpec {
	return append([]TunableSpec(nil), partitioningTunableSpecs...)
}

func (s *PartitioningScheduler) TunableValues() map[string]any {
	return map[string]any{
		TunableParallelismTarget: s.getParallelismTarget(),
	}
}

func (s *PartitioningScheduler) ApplyTunables(values map[string]any) error {
	specsByKey := make(map[string]TunableSpec, len(partitioningTunableSpecs))
	for _, spec := range partitioningTunableSpecs {
		specsByKey[spec.Key] = spec
	}

	updates := make(map[string]int, len(values))
	for key := range values {
		spec, ok := specsByKey[key]
		if !ok {
			return fmt.Errorf("unknown tunable %q", key)
		}
		switch key {
		case TunableParallelismTarget:
			n, err := tunableInt(values, key)
			if err != nil {
				return err
			}
			if err := validateInt(spec, n); err != nil {
				return err
			}
			updates[key] = n
		default:
			return fmt.Errorf("unknown tunable %q", key)
		}
	}

	for key, value := range updates {
		switch key {
		case TunableParallelismTarget:
			s.setParallelismTarget(value)
		default:
			return fmt.Errorf("unknown tunable %q", key)
		}
	}

	return nil
}

var _ TunableScheduler = (*PartitioningScheduler)(nil)
