package gcas

// GCAS is a content-addressible storage service that combines multiple CAS nodes into a single CAS.
// It uses erasure coding to provide efficient redundancy.
// The erasure coding used is Reed-Solomon coding.
type GCAS interface {
	CAS
	AddNode(node NamedCAS)
	RemoveNode(nodeName string)
	ReplaceNode(node NamedCAS) // replaces the node with the same name with the new node
}

type NamedCAS interface {
	CAS
	Name() string
}
