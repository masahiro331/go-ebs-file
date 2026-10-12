package ebsfile

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

const benchmarkBlockSize = 512 * 1024

// The cache is nil, so every ReadAt fetches each block it touches and the
// benchmarks measure the uncached read path.
func newBenchmarkReader(b *testing.B, blocks int32) (*io.SectionReader, *MockEBS) {
	b.Helper()

	data := make([]byte, int64(benchmarkBlockSize)*int64(blocks))
	for i := int32(0); i < blocks; i++ {
		data[int64(i)*benchmarkBlockSize] = byte(i)
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(benchmarkBlockSize),
		VolumeSize: aws.Int64(int64(len(data))),
		Latency:    100 * time.Microsecond,
	}

	reader, err := Open("benchmark-snapshot", context.Background(), nil, mock)
	if err != nil {
		b.Fatal(err)
	}
	return reader, mock
}

func BenchmarkReadAt_Sequential(b *testing.B) {
	reader, mock := newBenchmarkReader(b, 64)
	buf := make([]byte, 4*benchmarkBlockSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64((i % 16) * len(buf))
		if _, err := reader.ReadAt(buf, offset); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(mock.GetSnapshotBlockCount())/float64(b.N), "requests/op")
}

func BenchmarkReadAt_Random(b *testing.B) {
	const blocks = int32(64)
	reader, mock := newBenchmarkReader(b, blocks)
	rng := rand.New(rand.NewSource(1))
	buf := make([]byte, 4*1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(rng.Int31n(blocks)) * benchmarkBlockSize
		if _, err := reader.ReadAt(buf, offset); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(mock.GetSnapshotBlockCount())/float64(b.N), "requests/op")
}

func BenchmarkReadAt_CrossBlock(b *testing.B) {
	reader, mock := newBenchmarkReader(b, 2)
	buf := make([]byte, benchmarkBlockSize+1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := reader.ReadAt(buf, benchmarkBlockSize/2); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(mock.GetSnapshotBlockCount())/float64(b.N), "requests/op")
}
