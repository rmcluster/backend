package util

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestReadCloserWrapperClose(t *testing.T) {
	tests := []struct {
		name      string
		closer    func() error
		expectErr bool
		errMsg    string
	}{
		{
			name: "successful close",
			closer: func() error {
				return nil
			},
			expectErr: false,
		},
		{
			name: "close returns error",
			closer: func() error {
				return errors.New("close failed")
			},
			expectErr: true,
			errMsg:    "close failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapper := ReadCloserWrapper{
				Reader: bytes.NewReader([]byte("test")),
				Closer: tt.closer,
			}

			err := wrapper.Close()
			if tt.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if tt.expectErr && err.Error() != tt.errMsg {
				t.Errorf("expected error %q, got %q", tt.errMsg, err.Error())
			}
		})
	}
}

func TestReadCloserWrapperRead(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		readSize  int
		expectN   int
		expectErr bool
	}{
		{
			name:      "read full buffer",
			data:      []byte("hello world"),
			readSize:  11,
			expectN:   11,
			expectErr: false,
		},
		{
			name:      "read partial buffer",
			data:      []byte("hello world"),
			readSize:  5,
			expectN:   5,
			expectErr: false,
		},
		{
			name:      "read with EOF",
			data:      []byte("test"),
			readSize:  10,
			expectN:   4,
			expectErr: false, // EOF is not an error in Read
		},
		{
			name:      "read empty buffer",
			data:      []byte(""),
			readSize:  10,
			expectN:   0,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapper := ReadCloserWrapper{
				Reader: bytes.NewReader(tt.data),
				Closer: func() error { return nil },
			}

			buf := make([]byte, tt.readSize)
			n, err := wrapper.Read(buf)

			if tt.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.expectErr && err != nil && err != io.EOF {
				t.Errorf("expected no error, got %v", err)
			}
			if n != tt.expectN {
				t.Errorf("expected to read %d bytes, got %d", tt.expectN, n)
			}
		})
	}
}

func TestReadCloserWrapperIsReadCloser(t *testing.T) {
	// This test verifies that ReadCloserWrapper implements io.ReadCloser
	var _ io.ReadCloser = ReadCloserWrapper{
		Reader: bytes.NewReader([]byte("test")),
		Closer: func() error { return nil },
	}
}
