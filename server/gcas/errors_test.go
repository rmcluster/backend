package gcas

import "testing"

func TestHashNotFoundErrorMessage(t *testing.T) {
	e := HashNotFoundError{}
	if e.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestHashExistsErrorMessage(t *testing.T) {
	e := HashExistsError{}
	if e.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestDataCorruptErrorMessage(t *testing.T) {
	e := DataCorruptError{}
	if e.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestErrNoNodesMessage(t *testing.T) {
	e := ErrNoNodes{}
	if e.Error() == "" {
		t.Error("expected non-empty error message")
	}
}
