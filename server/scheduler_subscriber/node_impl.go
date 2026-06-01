package schedulersubscriber

import "github.com/rmcluster/backend/server/scheduling"

type node struct {
	id       string
	ip       string
	port     int
	maxSize  int64
	nickname string
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

// Nickname implements [scheduling.Node].
func (n *node) Nickname() string {
	return n.nickname
}

var _ scheduling.Node = (*node)(nil)
