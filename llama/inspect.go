package llama

import (
	"encoding/json"
	"fmt"
	"os"
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

	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return InspectInfo{}, fmt.Errorf("failed to pipe command: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return InspectInfo{}, fmt.Errorf("failed to start command: %v", err)
	}

	var info InspectInfo
	if err := json.NewDecoder(pipe).Decode(&info); err != nil {
		return InspectInfo{}, fmt.Errorf("failed to parse inspect output: %v", err)
	}

	return info, nil
}

func (r Llama) ModelSizeBytes(name string) (int64, error) {
	info, err := r.Inspect(name)
	if err != nil {
		return 0, err
	}
	stat, err := os.Stat(info.Path)
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
