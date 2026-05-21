package internal

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteShortString_ExceedsMaxLength(t *testing.T) {
	// WIRE-C1: writeShortString must return an error for strings > 255 bytes.
	// Previously it silently truncated the length via uint8 overflow while writing
	// the full string data, desynchronizing frame parsing.

	longStr := strings.Repeat("x", 256)
	buf := &bytes.Buffer{}

	err := writeShortString(buf, longStr)
	assert.Error(t, err, "writeShortString must return an error for strings exceeding 255 bytes")
	assert.Empty(t, buf.Bytes(), "buffer must not be modified when writeShortString returns an error")
}

func TestWriteShortString_Empty(t *testing.T) {
	buf := &bytes.Buffer{}

	err := writeShortString(buf, "")
	assert.NoError(t, err)

	// Verify: length byte is 0, no string data follows.
	assert.Equal(t, []byte{0}, buf.Bytes())

	// Round-trip: readShortString should return "".
	reader := bytes.NewReader(buf.Bytes())
	result, err := readShortString(reader)
	assert.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestWriteShortString_ExactMax(t *testing.T) {
	// A 255-byte string is the maximum valid short string.
	maxStr := strings.Repeat("a", 255)
	buf := &bytes.Buffer{}

	err := writeShortString(buf, maxStr)
	assert.NoError(t, err)

	// Verify: first byte is length (255), followed by the string content.
	data := buf.Bytes()
	assert.Equal(t, byte(255), data[0])
	assert.Equal(t, maxStr, string(data[1:]))
}

func TestWriteShortString_RoundTrip(t *testing.T) {
	// Verify that a written short string can be read back correctly.
	original := "hello-world"
	buf := &bytes.Buffer{}

	err := writeShortString(buf, original)
	assert.NoError(t, err)

	reader := bytes.NewReader(buf.Bytes())
	result, err := readShortString(reader)
	assert.NoError(t, err)
	assert.Equal(t, original, result)
}
