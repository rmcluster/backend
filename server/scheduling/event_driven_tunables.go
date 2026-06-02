package scheduling

import "fmt"

var eventDrivenTunableSpecs = []TunableSpec{
	{
		Key:         TunableMemoryTargetMultiplier,
		Label:       "Memory target multiplier",
		Description: "Scales model file size when auto-sizing nodes.",
		Kind:        TunableKindFloat,
		Min:         floatPtr(0),
	},
}

func (s *EventDrivenScheduler) SchedulerName() string {
	return "event_driven"
}

func (s *EventDrivenScheduler) TunableSpecs() []TunableSpec {
	return append([]TunableSpec(nil), eventDrivenTunableSpecs...)
}

func (s *EventDrivenScheduler) TunableValues() map[string]any {
	return map[string]any{
		TunableMemoryTargetMultiplier: s.getMemoryTargetMultiplier(),
	}
}

func (s *EventDrivenScheduler) ApplyTunables(values map[string]any) error {
	specsByKey := make(map[string]TunableSpec, len(eventDrivenTunableSpecs))
	for _, spec := range eventDrivenTunableSpecs {
		specsByKey[spec.Key] = spec
	}

	updates := make(map[string]float64, len(values))
	for key := range values {
		spec, ok := specsByKey[key]
		if !ok {
			return fmt.Errorf("unknown tunable %q", key)
		}
		switch key {
		case TunableMemoryTargetMultiplier:
			multiplier, err := tunableFloat(values, key)
			if err != nil {
				return err
			}
			if multiplier <= 0 {
				return fmt.Errorf("tunable %q must be > 0", key)
			}
			if err := validateFloat(spec, multiplier); err != nil {
				return err
			}
			updates[key] = multiplier
		default:
			return fmt.Errorf("unknown tunable %q", key)
		}
	}

	for key, value := range updates {
		switch key {
		case TunableMemoryTargetMultiplier:
			s.setMemoryTargetMultiplier(value)
		default:
			return fmt.Errorf("unknown tunable %q", key)
		}
	}

	return nil
}

var _ TunableScheduler = (*EventDrivenScheduler)(nil)

func (s *EventDrivenScheduler) getMemoryTargetMultiplier() float64 {
	s.tunableMu.RLock()
	defer s.tunableMu.RUnlock()
	return s.memoryTargetMultiplier
}

func (s *EventDrivenScheduler) setMemoryTargetMultiplier(multiplier float64) {
	s.tunableMu.Lock()
	s.memoryTargetMultiplier = multiplier
	s.tunableMu.Unlock()
}
