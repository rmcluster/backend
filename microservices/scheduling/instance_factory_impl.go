package scheduling

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/wk-y/rama-swap/llama"
)

func NewInstanceFactory(llmService *llama.Llama, lowestPort int) InstanceFactory {
	return &instanceFactoryImpl{
		llmService: llmService,
		lowestPort: lowestPort,
		usedPorts:  make(map[int]struct{}),
	}
}

type instanceFactoryImpl struct {
	sync.Mutex
	llmService     *llama.Llama
	lowestPort     int              // the lowest port to use
	usedPorts      map[int]struct{} // ports that are currently in use
	phaseCallback  func(model, phase string, progress float64)
	layersCallback func(layersOnGpu int)
}

// SetPhaseCallback implements [PhaseCallbackSetter].
func (i *instanceFactoryImpl) SetPhaseCallback(cb func(model, phase string, progress float64)) {
	i.Lock()
	defer i.Unlock()
	i.phaseCallback = cb
}

// SetLayersCallback implements [PhaseCallbackSetter].
func (i *instanceFactoryImpl) SetLayersCallback(cb func(layersOnGpu int)) {
	i.Lock()
	defer i.Unlock()
	i.layersCallback = cb
}

// StartInstance implements [InstanceFactory].
func (i *instanceFactoryImpl) StartInstance(model string, nodes []Node) (Instance, error) {
	log.Printf("Starting instance for model %s on %d nodes", model, len(nodes))
	i.Lock()
	if cb := i.phaseCallback; cb != nil {
		cb(model, PhaseStarting, 0)
	}
	i.Unlock()

	// build list of rpc nodes
	rpcNodes := make([]llama.RpcNode, len(nodes))
	rpcNodeSummaries := make([]string, len(nodes))
	for idx, node := range nodes {
		rpcNodes[idx] = llama.RpcNode{
			Ip:   node.Ip(),
			Port: node.Port(),
		}
		rpcNodeSummaries[idx] = formatNodeSummary(node)
	}
	log.Printf("Instance rpc nodes for model %s: %s", model, strings.Join(rpcNodeSummaries, ", "))
	tensorSplit := chooseTensorSplit(nodes)
	if len(tensorSplit) > 0 {
		log.Printf("Instance tensor split for model %s: %s", model, formatTensorSplit(tensorSplit))
	}

	// find the lowest port that is not used
	port := i.lowestPort
	for {
		if _, ok := i.usedPorts[port]; !ok {
			break
		}
		port++
	}

	// start the llama server
	startupLog := &startupLogBuffer{}
	cmd, err := func() (*exec.Cmd, error) {
		// guard critical section
		i.Lock()
		defer i.Unlock()
		offloadLayers := chooseOffloadLayers(nodes)

		cmd := i.llmService.ServeCommand(context.Background(), llama.ServeArgs{
			Model:         model,
			RpcNodes:      rpcNodes,
			Port:          port,
			OffloadLayers: &offloadLayers,
			TensorSplit:   tensorSplit,
			NoHost:        len(rpcNodes) > 0,
		})
		cmd.Stdout = newProcessLogWriter(model, "stdout", nil, startupLog.Add)
		// i is already locked here — read callbacks directly, no re-lock
		cmd.Stderr = newProcessLogWriter(model, "stderr", makePhaseDetector(model, i.phaseCallback, i.layersCallback), startupLog.Add)

		err := cmd.Start()
		if err != nil {
			return nil, err
		}

		// mark port as used
		i.usedPorts[port] = struct{}{}

		return cmd, err
	}()

	if err != nil {
		return nil, err
	}
	log.Printf("Started instance process for model %s on port %d", model, port)
	log.Printf("Instance command: %s", strings.Join(cmd.Args, " "))

	dead := make(chan struct{})

	// wait for instance to die, then free port
	go func() {
		cmd.Wait()
		close(dead)

		i.Lock()
		delete(i.usedPorts, port)
		i.Unlock()
	}()

	// return the new instance
	return &instanceImpl{
		process:    cmd.Process,
		port:       port,
		dead:       dead,
		model:      model,
		startupLog: startupLog,
		rpcNodes:   rpcNodeSummaries,
	}, nil
}

func formatNodeSummary(node Node) string {
	model := node.HardwareModel()
	if model == "" {
		model = "unknown-model"
	}
	return fmt.Sprintf("%s (%s @ %s:%d, max=%d)", node.Id(), model, node.Ip(), node.Port(), node.MaxSize())
}

func chooseOffloadLayers(nodes []Node) int {
	const defaultOffloadLayers = 99
	if override := strings.TrimSpace(os.Getenv("LLAMA_OFFLOAD_LAYERS")); override != "" {
		value, err := strconv.Atoi(override)
		if err != nil {
			log.Printf("Invalid LLAMA_OFFLOAD_LAYERS=%q; falling back to auto offload selection", override)
		} else {
			return value
		}
	}

	if len(nodes) == 0 {
		return 0
	}
	return defaultOffloadLayers
}

func chooseTensorSplit(nodes []Node) []float64 {
	if len(nodes) < 2 {
		return nil
	}

	split := make([]float64, len(nodes))
	var total float64
	for idx, node := range nodes {
		if node.MaxSize() <= 0 {
			return nil
		}
		split[idx] = float64(node.MaxSize())
		total += split[idx]
	}
	if total <= 0 {
		return nil
	}
	for idx := range split {
		split[idx] /= total
	}
	return split
}

func formatTensorSplit(split []float64) string {
	parts := make([]string, len(split))
	for idx, weight := range split {
		parts[idx] = fmt.Sprintf("%.4f", weight)
	}
	return strings.Join(parts, ",")
}

type processLogWriter struct {
	model  string
	stream string
	buffer bytes.Buffer
	onLine func(string) // called with each complete trimmed line
	onLog  func(stream string, line string)
}

func newProcessLogWriter(model string, stream string, onLine func(string), onLog func(stream string, line string)) *processLogWriter {
	return &processLogWriter{model: model, stream: stream, onLine: onLine, onLog: onLog}
}

// reProgressPct matches llama.cpp download progress lines, e.g. "45.2% (123456 / 272060416 bytes)"
var reProgressPct = regexp.MustCompile(`(\d+(?:\.\d+)?)%`)

// reOffloadLayers matches lines like "llm_load_tensors: offloading 32 repeating layers to GPU"
// and "llm_load_tensors: offloaded 32/33 layers to GPU".
// We keep the parser broad and let the UI decide the final device-neutral label.
var reOffloadLayers = regexp.MustCompile(`offload(?:ing|ed)\s+(\d+)`)

func (w *processLogWriter) Write(p []byte) (int, error) {
	// Treat \r as a line terminator so llama.cpp in-place progress updates are
	// each dispatched as individual lines rather than accumulated silently.
	normalised := bytes.ReplaceAll(p, []byte{'\r'}, []byte{'\n'})
	w.buffer.Write(normalised)
	for {
		line, err := w.buffer.ReadString('\n')
		if err == bytes.ErrTooLarge {
			break
		}
		if err != nil {
			if w.buffer.Len() == 0 {
				break
			}
			remaining := strings.TrimSpace(w.buffer.String())
			if remaining != "" {
				log.Printf("[llama %s %s] %s", w.model, w.stream, remaining)
				if w.onLog != nil {
					w.onLog(w.stream, remaining)
				}
				if w.onLine != nil {
					w.onLine(remaining)
				}
			}
			w.buffer.Reset()
			break
		}
		line = strings.TrimRight(line, "\n\r")
		if line != "" {
			log.Printf("[llama %s %s] %s", w.model, w.stream, line)
			if w.onLog != nil {
				w.onLog(w.stream, line)
			}
			if w.onLine != nil {
				w.onLine(line)
			}
		}
	}
	return len(p), nil
}

// makePhaseDetector returns an onLine hook that calls phaseCb whenever a known
// loading phase is detected, and layersCb when the offloaded-layer count is known.
func makePhaseDetector(model string, phaseCb func(model, phase string, progress float64), layersCb func(int)) func(string) {
	if phaseCb == nil && layersCb == nil {
		return nil
	}
	firstLine := true
	return func(line string) {
		if phaseCb != nil {
			if firstLine {
				firstLine = false
				phaseCb(model, PhaseInitializing, 0)
			}
			switch {
			case strings.Contains(line, ": downloading from "):
				phaseCb(model, PhaseDownloading, 0)
			case strings.Contains(line, "downloading") && strings.Contains(line, "%"):
				var pct float64
				if m := reProgressPct.FindStringSubmatch(line); m != nil {
					pct, _ = strconv.ParseFloat(m[1], 64)
				}
				phaseCb(model, PhaseDownloading, pct)
			case strings.Contains(line, "load_model: loading model"),
				strings.Contains(line, "main: loading model"):
				phaseCb(model, PhaseLoading, 0)
			case strings.Contains(line, "warming up"):
				phaseCb(model, PhaseWarmingUp, 0)
			case strings.Contains(line, "server is listening"):
				phaseCb(model, PhaseReady, 0)
			}
		}
		if layersCb != nil && strings.Contains(line, "offload") {
			if m := reOffloadLayers.FindStringSubmatch(line); m != nil {
				if n, err := strconv.Atoi(m[1]); err == nil {
					layersCb(n)
				}
			}
		}
	}
}

var _ InstanceFactory = (*instanceFactoryImpl)(nil)
