package llama

type Llama struct {
	Command []string
}

// checkValidity checks that the Ramalama configuration is valid.
// A valid config must have a non-empty Command slice.
func (r Llama) checkValidity() error {
	if len(r.Command) == 0 {
		return ErrEmptyCommand{}
	}
	return nil
}

type Model struct {
	Name     string
	Modified string
	Size     int
}

func (c Llama) GetModels() ([]Model, error) {
	return []Model{
		// ~0.6B
		{Name: "hf:unsloth/Qwen3-0.6B-GGUF:UD-Q4_K_XL"},
		// ~1B
		{Name: "hf:unsloth/gemma-3-1b-it-GGUF:gemma-3-1b-it-Q4_K_M.gguf"},
		{Name: "hf:unsloth/Llama-3.2-1B-Instruct-GGUF:Llama-3.2-1B-Instruct-Q4_K_M.gguf"},
		// ~1.5-2B
		{Name: "hf:unsloth/Qwen3-1.7B-GGUF:Qwen3-1.7B-Q4_K_M.gguf"},
		{Name: "hf:unsloth/SmolLM2-1.7B-Instruct-GGUF:SmolLM2-1.7B-Instruct-Q4_K_M.gguf"},
		// ~1.5B (reasoning)
		{Name: "hf:unsloth/DeepSeek-R1-Distill-Qwen-1.5B-GGUF:DeepSeek-R1-Distill-Qwen-1.5B-Q4_K_M.gguf"},
		// ~3B
		{Name: "hf:unsloth/Llama-3.2-3B-Instruct-GGUF:Llama-3.2-3B-Instruct-Q4_K_M.gguf"},
		{Name: "hf:unsloth/DeepSeek-R1-Distill-Qwen-7B-GGUF:DeepSeek-R1-Distill-Qwen-7B-Q4_K_M.gguf"},
		// ~4B
		{Name: "hf:unsloth/Qwen3-4B-GGUF:Qwen3-4B-Q4_K_M.gguf"},
		{Name: "hf:unsloth/gemma-3-4b-it-GGUF:gemma-3-4b-it-Q4_K_M.gguf"},
	}, nil
}
