package internal

import (
	"testing"

	"github.com/aleybovich/carrot-mq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeKeySegment_NoSpecialChars(t *testing.T) {
	// Simple names pass through with minimal changes
	encoded := encodeKeySegment("my-queue")
	assert.Equal(t, "my-queue", encoded)
}

func TestEncodeKeySegment_ColonEncoded(t *testing.T) {
	encoded := encodeKeySegment("us-east:order")
	assert.Contains(t, encoded, "%3A")
	assert.NotContains(t, encoded, ":")
}

func TestEncodeKeySegment_PercentEncoded(t *testing.T) {
	// Percent itself must be encoded to avoid ambiguity
	encoded := encodeKeySegment("100%done")
	assert.Equal(t, "100%25done", encoded)
}

func TestDecodeKeySegment_RoundTrip(t *testing.T) {
	cases := []string{
		"simple",
		"with:colon",
		"with:multiple:colons",
		"with%percent",
		"with%3Aliteral",
		"/",
		"complex:name%with/slashes",
		"", // empty name
	}
	for _, original := range cases {
		encoded := encodeKeySegment(original)
		decoded, err := decodeKeySegment(encoded)
		require.NoError(t, err)
		assert.Equal(t, original, decoded, "round-trip failed for %q", original)
	}
}

func TestCompositeKey_SplitRoundTrip(t *testing.T) {
	vhost := "my:vhost"
	queue := "order:queue"

	key := compositeKey(vhost, queue)
	parts, err := splitCompositeKey(key, 2)
	require.NoError(t, err)
	assert.Equal(t, vhost, parts[0])
	assert.Equal(t, queue, parts[1])
}

func TestCompositeKey_ThreeParts(t *testing.T) {
	a, b, c := "a:x", "b:y", "c:z"
	key := compositeKey(a, b, c)
	parts, err := splitCompositeKey(key, 3)
	require.NoError(t, err)
	assert.Equal(t, a, parts[0])
	assert.Equal(t, b, parts[1])
	assert.Equal(t, c, parts[2])
}

func TestSplitCompositeKey_WrongPartCount(t *testing.T) {
	key := compositeKey("a", "b")
	_, err := splitCompositeKey(key, 3)
	require.Error(t, err)
}

func TestKeyBuilders_NoCollision(t *testing.T) {
	// These two must produce different keys — the original bug
	key1 := BindingKey("a", "b", "c:d", "e")
	key2 := BindingKey("a", "b:c", "d", "e")
	assert.NotEqual(t, key1, key2, "BindingKey must not collide on colon-containing names")
}

func TestKeyBuilders_NoCollision_ExchangeKey(t *testing.T) {
	key1 := ExchangeKey("vh:1", "ex")
	key2 := ExchangeKey("vh", "1:ex")
	assert.NotEqual(t, key1, key2)
}

func TestKeyBuilders_NoCollision_QueueKey(t *testing.T) {
	key1 := QueueKey("vh:1", "q")
	key2 := QueueKey("vh", "1:q")
	assert.NotEqual(t, key1, key2)
}

func TestKeyBuilders_NoCollision_MessageKey(t *testing.T) {
	key1 := MessageKey("v", "q:1", "msg")
	key2 := MessageKey("v", "q", "1:msg")
	assert.NotEqual(t, key1, key2)
}

func TestKeyBuilders_NoCollision_MessageIndexKey(t *testing.T) {
	key1 := MessageIndexKey("v:h", "queue")
	key2 := MessageIndexKey("v", "h:queue")
	assert.NotEqual(t, key1, key2)
}

func TestKeyBuilders_PrefixPreserved(t *testing.T) {
	// Keys must start with the correct prefix for storage scanning
	assert.Contains(t, VHostKey("test"), storage.KeyPrefixVHost)
	assert.Contains(t, ExchangeKey("v", "e"), storage.KeyPrefixExchange)
	assert.Contains(t, QueueKey("v", "q"), storage.KeyPrefixQueue)
	assert.Contains(t, BindingKey("v", "e", "q", "rk"), storage.KeyPrefixBinding)
	assert.Contains(t, MessageKey("v", "q", "m"), storage.KeyPrefixMessage)
	assert.Contains(t, MessageIndexKey("v", "q"), storage.KeyPrefixMsgIndex)
}

func TestKeyBuilders_SimpleNamesUnchangedStructure(t *testing.T) {
	// For simple names without special chars, keys should still be readable
	key := ExchangeKey("myvhost", "myexchange")
	assert.Equal(t, storage.KeyPrefixExchange+"myvhost:myexchange", key)
}

func TestVHostKey_Slash(t *testing.T) {
	// Default vhost "/" must encode the slash
	key := VHostKey("/")
	decoded, err := decodeKeySegment(key[len(storage.KeyPrefixVHost):])
	require.NoError(t, err)
	assert.Equal(t, "/", decoded)
}
