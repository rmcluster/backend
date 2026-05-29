package server

import (
	"fmt"
	"time"

	"github.com/rmcluster/backend/llama"
)

func convertModelList(ramaModels []llama.Model) ([]Model, error) {
	var models []Model
	for _, ramaModel := range ramaModels {
		var created int
		if ramaModel.Modified != "" {
			t, err := time.Parse(time.RFC3339, ramaModel.Modified)
			if err != nil {
				return nil, fmt.Errorf("failed to parse model timestamp %#v: %v", ramaModel.Modified, err)
			}
			created = int(t.Unix())
		}
		models = append(models, Model{
			Id:      ramaModel.Name,
			Object:  "model",
			Created: created,
			OwnedBy: "rama-swap",
		})
	}
	return models, nil
}
