package llama

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
)

type InspectInfo struct {
	Endianness int64         `json:"Endianness"`
	Format     string        `json:"Format"`
	Metadata   ModelMetadata `json:"Metadata"`
	Name       string        `json:"Name"`
	Path       string        `json:"Path"`
	Registry   string        `json:"Registry"`
	Tensors    []struct {
		Dimensions  []int64 `json:"dimensions"`
		NDimensions int64   `json:"n_dimensions"`
		Name        string  `json:"name"`
		Offset      int64   `json:"offset"`
		Type        string  `json:"type"`
	} `json:"Tensors"`
	Version int64 `json:"Version"`
}

type ModelMetadata struct {
	GeneralArchitecture *string `json:"general.architecture"`
	GeneralSizeLabel    *string `json:"general.size_label"`
}

func (r Llama) Inspect(name string) (InspectInfo, error) {
	if err := r.checkValidity(); err != nil {
		return InspectInfo{}, err
	}

	cliArgs := slices.Concat(r.Command[1:], []string{"inspect", "--json", "--all", name})
	cmd := exec.Command(r.Command[0], cliArgs...)

	pipe, pipeErr := cmd.StdoutPipe()
	if TestForceInspectPipeError {
		pipeErr = fmt.Errorf("forced")
	}
	if pipeErr != nil {
		return InspectInfo{}, fmt.Errorf("failed to pipe command: %v", pipeErr)
	}

	var startErr error
	if TestForceInspectStartError {
		startErr = fmt.Errorf("forced")
	} else {
		startErr = cmd.Start()
	}
	if startErr != nil {
		return InspectInfo{}, fmt.Errorf("failed to start command: %v", startErr)
	}

	var info InspectInfo
	if err := json.NewDecoder(pipe).Decode(&info); err != nil {
		return InspectInfo{}, fmt.Errorf("failed to parse inspect output: %v", err)
	}

	return info, nil
}
