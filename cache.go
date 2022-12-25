package ebsfile

var (
	_ Cache[string, []byte] = &mockCache[string, []byte]{}
)

type Cache[K string, V any] interface {
	// Add cache data
	Add(key K, value V) bool

	// Get returns key's value from the cache
	Get(key K) (value V, ok bool)
}

type mockCache[K string, V []byte] struct{}

func (c *mockCache[K, V]) Add(_ K, _ V) bool {
	return false
}

func (c *mockCache[K, V]) Get(_ K) (v V, evicted bool) {
	return
}
