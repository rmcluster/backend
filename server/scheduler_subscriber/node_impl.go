package schedulersubscriber

import "github.com/rmcluster/backend/server/scheduling"

type node struct {
	id            string
	ip            string
	port          int
	storagePort   int
	maxSize       int64
	nickname      string
	hardwareModel string
	battery       float64
	temperature   float64
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

// StoragePort implements [scheduling.Node].
func (n *node) StoragePort() int {
	return n.storagePort
}

// HardwareModel implements [scheduling.Node].
func (n *node) HardwareModel() string {
	return n.hardwareModel
}

// Battery implements [scheduling.Node].
func (n *node) Battery() float64 {
	return n.battery
}

// Temperature implements [scheduling.Node].
func (n *node) Temperature() float64 {
	return n.temperature
}

var _ scheduling.Node = (*node)(nil)
