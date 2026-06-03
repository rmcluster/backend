package llama

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestErrEmptyCommand_Error(t *testing.T) {
	var e ErrEmptyCommand
	if got := e.Error(); got == "" {
		t.Fatalf("empty Error() string")
	}
}

func TestCheckValidity(t *testing.T) {
	lm0 := Llama{Command: nil}
	if err := lm0.checkValidity(); err == nil {
		t.Fatalf("expected error for empty command")
	}
	lm1 := Llama{Command: []string{"prog"}}
	if err := lm1.checkValidity(); err != nil {
		t.Fatalf("unexpected error for non-empty command: %v", err)
	}
}

func TestGetModels(t *testing.T) {
	var c Llama
	ms, err := c.GetModels()
	if err != nil {
		t.Fatalf("GetModels returned error: %v", err)
	}
	if len(ms) == 0 {
		t.Fatalf("expected non-empty model list")
	}
	if !strings.HasPrefix(ms[0].Name, "hf:") {
		t.Fatalf("unexpected first model name: %s", ms[0].Name)
	}
}

func TestCacheListEntry_ParseCachedModelRef(t *testing.T) {
	entry, ok := cacheListEntry("   1. owner/repo:Q4_K_M")
	if !ok || entry != "owner/repo:Q4_K_M" {
		t.Fatalf("cacheListEntry failed, got %q ok=%v", entry, ok)
	}

	// no dot
	if _, ok := cacheListEntry("no dot here"); ok {
		t.Fatalf("expected false for malformed line")
	}

	repo, quant, ok := parseCachedModelRef("owner/repo:Q4_K_M")
	if !ok || repo != "owner/repo" || quant != "Q4_K_M" {
		t.Fatalf("parseCachedModelRef failed, got %q %q ok=%v", repo, quant, ok)
	}

	// trimming and uppercasing
	repo, quant, ok = parseCachedModelRef(" owner/repo : q4_k_m ")
	if !ok || quant != "Q4_K_M" {
		t.Fatalf("parseCachedModelRef trimming/upper failed, got %q %q ok=%v", repo, quant, ok)
	}

	// missing parts
	if _, _, ok := parseCachedModelRef("no-colon"); ok == true {
		t.Fatalf("expected false for missing colon")
	}
}

func TestGetCachedModels_BasicAndEmpty(t *testing.T) {
	// empty command -> validation error
	if _, err := (Llama{Command: nil}).GetCachedModels(); err == nil {
		t.Fatalf("expected error for empty command")
	}

	// use /bin/true which returns success and no output; should parse to empty slice
	lm := Llama{Command: []string{"true"}}
	models, err := lm.GetCachedModels()
	if err != nil {
		t.Fatalf("GetCachedModels failed: %v", err)
	}
	if len(models) != 0 {
		t.Fatalf("expected zero cached models from true, got %d", len(models))
	}
}

func TestInspect_ErrorPath(t *testing.T) {
	// running `true` will produce no JSON -> parse error
	lm := Llama{Command: []string{"true"}}
	if _, err := lm.Inspect("something"); err == nil || !strings.Contains(err.Error(), "failed to parse inspect output") {
		t.Fatalf("expected parse error from Inspect, got: %v", err)
	}
}

func TestServeCommand_Flags(t *testing.T) {
	ctx := context.Background()
	c := Llama{Command: []string{"llama-server", "-v"}}

	alias := "myalias"
	cmd := c.ServeCommand(ctx, ServeArgs{
		Model:    "mymodel",
		Port:     8080,
		Alias:    &alias,
		RpcNodes: []RpcNode{{Ip: "10.0.0.1", Port: 9000}, {Ip: "10.0.0.2", Port: 9001}},
	})

	// basic expectations: program and some flags present
	if len(cmd.Args) == 0 || cmd.Args[0] != "llama-server" {
		t.Fatalf("unexpected program in cmd.Args: %v", cmd.Args)
	}

	argv := cmd.Args[1:]
	// ensure some expected flags exist in order
	wantSeq := []string{"-ngl", "99", "-c", "4096", "--rpc", "10.0.0.1:9000,10.0.0.2:9001", "-n", "myalias", "--port", "8080", "--model", "mymodel"}
	if !reflect.DeepEqual(argv[len(argv)-len(wantSeq):], wantSeq) {
		t.Fatalf("ServeCommand args do not end with expected sequence; got %v want suffix %v", argv, wantSeq)
	}

	// hf model path
	cmd2 := c.ServeCommand(ctx, ServeArgs{Model: "hf:abcd", Port: 1234})
	if !containsAll(cmd2.Args, []string{"-hf", "abcd", "--port", "1234"}) {
		t.Fatalf("ServeCommand did not include hf flags: %v", cmd2.Args)
	}
}

func containsAll(hay []string, needles []string) bool {
	for i := 0; i < len(hay); i++ {
		ok := true
		for j := 0; j < len(needles) && i+j < len(hay); j++ {
			if hay[i+j] != needles[j] {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func TestGetCachedModels_ParseOutputAndInspect_Success(t *testing.T) {
	dir := t.TempDir()
	// script for cached models
	script1 := dir + "/cached.sh"
	content1 := "#!/bin/sh\nprintf '   1. owner/repo:Q4_K_M\\n   2. owner2/repo2: q4_k_m\\n'\n"
	if err := writeExecutable(script1, []byte(content1)); err != nil {
		t.Fatalf("failed to write script: %v", err)
	}

	lm := Llama{Command: []string{"/bin/sh", script1}}
	cms, err := lm.GetCachedModels()
	if err != nil {
		t.Fatalf("GetCachedModels failed: %v", err)
	}
	if len(cms) != 2 {
		t.Fatalf("expected 2 cached models, got %d", len(cms))
	}
	if cms[0].Repo != "owner/repo" || cms[0].Quant != "Q4_K_M" {
		t.Fatalf("unexpected parsed model: %#v", cms[0])
	}

	// script for inspect JSON
	script2 := dir + "/inspect.sh"
	json := `{"Endianness":1,"Format":"fmt","Metadata":{"general.architecture":"arch","general.size_label":"XL"},"Name":"mname","Path":"mp","Registry":"reg","Tensors":[],"Version":2}`
	content2 := "#!/bin/sh\necho '" + json + "'\n"
	if err := writeExecutable(script2, []byte(content2)); err != nil {
		t.Fatalf("failed to write inspect script: %v", err)
	}

	lm2 := Llama{Command: []string{"/bin/sh", script2}}
	info, err := lm2.Inspect("ignored")
	if err != nil {
		t.Fatalf("Inspect failed: %v", err)
	}
	if info.Name != "mname" || info.Format != "fmt" || info.Version != 2 {
		t.Fatalf("unexpected inspect info: %#v", info)
	}
}

func writeExecutable(path string, data []byte) error {
	if err := os.WriteFile(path, data, 0700); err != nil {
		return err
	}
	return nil
}

func TestGetCachedModels_CommandFailure(t *testing.T) {
	// use a command that exits non-zero
	lm := Llama{Command: []string{"false"}}
	if _, err := lm.GetCachedModels(); err == nil || !strings.Contains(err.Error(), "failed to list cached models") {
		t.Fatalf("expected failing command error, got: %v", err)
	}
}

func TestGetCachedModels_ForcedPaths(t *testing.T) {
	// force output error
	TestForceCacheListOutputError = true
	lm := Llama{Command: []string{"true"}}
	if _, err := lm.GetCachedModels(); err == nil || !strings.Contains(err.Error(), "forced cache list error") {
		TestForceCacheListOutputError = false
		t.Fatalf("expected forced cache list error, got: %v", err)
	}
	TestForceCacheListOutputError = false

	// force parse error
	TestForceCacheParseError = true
	lm2 := Llama{Command: []string{"true"}}
	if _, err := lm2.GetCachedModels(); err == nil || !strings.Contains(err.Error(), "forced parse error") {
		TestForceCacheParseError = false
		t.Fatalf("expected forced parse error, got: %v", err)
	}
	TestForceCacheParseError = false
}

func TestInspect_ForcedPaths(t *testing.T) {
	TestForceInspectPipeError = true
	lm3 := Llama{Command: []string{"true"}}
	if _, err := lm3.Inspect("x"); err == nil || !strings.Contains(err.Error(), "failed to pipe command") {
		TestForceInspectPipeError = false
		t.Fatalf("expected forced pipe error, got: %v", err)
	}
	TestForceInspectPipeError = false

	TestForceInspectStartError = true
	lm4 := Llama{Command: []string{"true"}}
	if _, err := lm4.Inspect("x"); err == nil || !strings.Contains(err.Error(), "failed to start command") {
		TestForceInspectStartError = false
		t.Fatalf("expected forced start error, got: %v", err)
	}
	TestForceInspectStartError = false
}

func TestGetCachedModels_MalformedLines(t *testing.T) {
	dir := t.TempDir()
	script := dir + "/mixed.sh"
	// produces: a line without dot, a valid entry, an entry without colon, an entry with empty repo, another valid
	content := "#!/bin/sh\nprintf 'no dot here\\n   1. owner/repo:Q4_K_M\\n   2. owneronly\\n   3. :Q4\\n   4. owner2/repo2:q4_k_m\\n'\n"
	if err := writeExecutable(script, []byte(content)); err != nil {
		t.Fatalf("failed to write mixed script: %v", err)
	}

	lm := Llama{Command: []string{"/bin/sh", script}}
	cms, err := lm.GetCachedModels()
	if err != nil {
		t.Fatalf("GetCachedModels failed on mixed output: %v", err)
	}
	if len(cms) != 2 {
		t.Fatalf("expected 2 parsed models, got %d: %#v", len(cms), cms)
	}
}

func TestInspect_CheckValidity(t *testing.T) {
	// empty command should return ErrEmptyCommand via checkValidity
	lm := Llama{Command: nil}
	if _, err := lm.Inspect("x"); err == nil {
		t.Fatalf("expected error for empty command in Inspect")
	}
}

func TestGetCachedModels_ScannerErr(t *testing.T) {
	dir := t.TempDir()
	script := dir + "/huge.sh"
	// create a line > 70000 chars to exceed bufio.Scanner buffer
	long := make([]byte, 100000)
	for i := range long {
		long[i] = 'a'
	}
	content := "#!/bin/sh\nprintf '1. " + string(long) + "\\n'\n"
	if err := writeExecutable(script, []byte(content)); err != nil {
		t.Fatalf("failed to write huge script: %v", err)
	}

	lm := Llama{Command: []string{"/bin/sh", script}}
	if _, err := lm.GetCachedModels(); err == nil || !strings.Contains(err.Error(), "failed to parse cached models") {
		t.Fatalf("expected scanner parse error, got: %v", err)
	}
}
