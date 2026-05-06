package gcas

type ErrNoNodes struct {
}

func (e ErrNoNodes) Error() string {
	return "no nodes available"
}
