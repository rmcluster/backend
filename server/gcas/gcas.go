package gcas

// GCAS is a content-addressible storage service that combines multiple CAS nodes into a single CAS.
// It uses erasure coding to provide efficient redundancy.
// The erasure coding used is Reed-Solomon coding.
type GCAS interface {
	CAS
	AddNode(node NamedCAS)
	RemoveNode(nodeName string)
}

type NamedCAS interface {
	CAS
	Name() string
}
