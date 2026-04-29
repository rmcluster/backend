package schedulersubscriber

import "github.com/wk-y/rama-swap/microservices/scheduling"

type node struct {
	id      string
	ip      string
	port    int
	maxSize int64
}

// Id implements [scheduling.Node].
func (n *node) Id() string {
	return n.id
}

// Ip implements [scheduling.Node].
func (n *node) Ip() string {
	return n.ip
}

// Port implements [scheduling.Node].
func (n *node) Port() int {
	return n.port
}

// MaxSize implements [scheduling.Node].
func (n *node) MaxSize() int64 {
	return n.maxSize
}

var _ scheduling.Node = (*node)(nil)
