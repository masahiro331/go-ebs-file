package ebsfile

var (
	_ Cache[string, []byte] = &mockCache[string, []byte]{}
	_ Cache[int64, []byte]  = &mockCache[int64, []byte]{}
)

type Cache[K comparable, V any] interface {
	// Add cache data
	Add(key K, value V) bool

	// Get returns key's value from the cache
	Get(key K) (value V, ok bool)
}

type mockCache[K comparable, V any] struct{}

func (c *mockCache[K, V]) Add(_ K, _ V) bool {
	return false
}

func (c *mockCache[K, V]) Get(_ K) (v V, ok bool) {
	return
}
