package ebsfile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ebs"
)

func TestWalkSnapshotBlocks_Pagination(t *testing.T) {
	blockSize := int32(16)
	numBlocks := int32(10)
	volumeSize := int64(numBlocks) * int64(blockSize) // 160 bytes

	data := make([]byte, volumeSize)
	for i := int32(0); i < numBlocks; i++ {
		data[int64(i)*int64(blockSize)] = byte(i)
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
		PageSize:   3, // 3 blocks per page → 4 pages for 10 blocks
	}

	sr, err := Open("snap-pagination", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	if sr.Size() != volumeSize {
		t.Fatalf("expected size %d, got %d", volumeSize, sr.Size())
	}

	// Read first byte of each block to verify all pages were collected
	buf := make([]byte, 1)
	for i := int32(0); i < numBlocks; i++ {
		off := int64(i) * int64(blockSize)
		n, err := sr.ReadAt(buf, off)
		if err != nil {
			t.Fatalf("block %d: unexpected error: %v", i, err)
		}
		if n != 1 || buf[0] != byte(i) {
			t.Errorf("block %d: got n=%d, buf[0]=0x%02X, want 0x%02X", i, n, buf[0], byte(i))
		}
	}
}

func TestWalkSnapshotBlocks_PaginationWithUnallocated(t *testing.T) {
	blockSize := int32(16)
	numBlocks := int32(8)
	volumeSize := int64(numBlocks) * int64(blockSize) // 128 bytes

	data := make([]byte, volumeSize)
	allocated := make(map[int32]bool)
	// Allocate only even blocks
	for i := int32(0); i < numBlocks; i++ {
		if i%2 == 0 {
			allocated[i] = true
			data[int64(i)*int64(blockSize)] = 0xAA
		}
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
		Allocated:  allocated,
		PageSize:   2, // 2 blocks per page → multiple pages
	}

	sr, err := Open("snap-unalloc", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	// Allocated block 0: should have marker
	buf := make([]byte, 1)
	n, err := sr.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 || buf[0] != 0xAA {
		t.Errorf("block 0: got n=%d, buf[0]=0x%02X, want 0xAA", n, buf[0])
	}

	// Unallocated block 1: should be zero
	buf[0] = 0xFF
	n, err = sr.ReadAt(buf, int64(blockSize))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 || buf[0] != 0x00 {
		t.Errorf("block 1: got n=%d, buf[0]=0x%02X, want 0x00", n, buf[0])
	}
}

func TestReadAt_UnallocatedBlockZeroFill(t *testing.T) {
	blockSize := int32(16)
	volumeSize := int64(48) // 3 blocks

	data := make([]byte, volumeSize)
	// Fill allocated blocks with non-zero data
	for i := 0; i < int(blockSize); i++ {
		data[i] = 0xAA                  // block 0
		data[int(blockSize)*2+i] = 0xBB // block 2
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
		Allocated:  map[int32]bool{0: true, 2: true}, // block 1 is unallocated
	}

	sr, err := Open("snap-test", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	// Read from unallocated block 1: should be all zeros
	buf := make([]byte, blockSize)
	// Pre-fill with garbage to verify zero-fill
	for i := 0; i < len(buf); i++ {
		buf[i] = 0xFF
	}
	n, err := sr.ReadAt(buf, int64(blockSize))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != int(blockSize) {
		t.Fatalf("expected n=%d, got %d", blockSize, n)
	}
	for i, b := range buf {
		if b != 0 {
			t.Errorf("buf[%d] = 0x%02X, want 0x00", i, b)
		}
	}
}

func TestReadAt_CrossBlockBoundary(t *testing.T) {
	blockSize := int32(16)
	volumeSize := int64(48) // 3 blocks

	data := make([]byte, volumeSize)
	// Fill with distinct patterns per block
	for i := 0; i < int(blockSize); i++ {
		data[i] = 0xAA
		data[int(blockSize)+i] = 0xBB
		data[int(blockSize)*2+i] = 0xCC
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
	}

	sr, err := Open("snap-test", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	// Read across block 0 and block 1 boundary
	// Start at offset 8 (middle of block 0), read 16 bytes (spans into block 1)
	buf := make([]byte, 16)
	n, err := sr.ReadAt(buf, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 16 {
		t.Fatalf("expected n=16, got %d", n)
	}
	// First 8 bytes should be from block 0 (0xAA)
	for i := 0; i < 8; i++ {
		if buf[i] != 0xAA {
			t.Errorf("buf[%d] = 0x%02X, want 0xAA", i, buf[i])
		}
	}
	// Next 8 bytes should be from block 1 (0xBB)
	for i := 8; i < 16; i++ {
		if buf[i] != 0xBB {
			t.Errorf("buf[%d] = 0x%02X, want 0xBB", i, buf[i])
		}
	}
}

func TestReadAt_CrossBlockBoundaryWithUnallocated(t *testing.T) {
	blockSize := int32(16)
	volumeSize := int64(48) // 3 blocks

	data := make([]byte, volumeSize)
	for i := 0; i < int(blockSize); i++ {
		data[i] = 0xAA
		data[int(blockSize)*2+i] = 0xCC
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
		Allocated:  map[int32]bool{0: true, 2: true}, // block 1 unallocated
	}

	sr, err := Open("snap-test", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	// Read across block 0, unallocated block 1, and block 2
	buf := make([]byte, 32)
	for i := 0; i < len(buf); i++ {
		buf[i] = 0xFF
	}
	n, err := sr.ReadAt(buf, 8) // offset 8 in block 0
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 32 {
		t.Fatalf("expected n=32, got %d", n)
	}
	// [0..7]: block 0 tail (0xAA)
	for i := 0; i < 8; i++ {
		if buf[i] != 0xAA {
			t.Errorf("buf[%d] = 0x%02X, want 0xAA", i, buf[i])
		}
	}
	// [8..23]: unallocated block 1 (0x00)
	for i := 8; i < 24; i++ {
		if buf[i] != 0x00 {
			t.Errorf("buf[%d] = 0x%02X, want 0x00", i, buf[i])
		}
	}
	// [24..31]: block 2 head (0xCC)
	for i := 24; i < 32; i++ {
		if buf[i] != 0xCC {
			t.Errorf("buf[%d] = 0x%02X, want 0xCC", i, buf[i])
		}
	}
}

func TestReadAt_EOF(t *testing.T) {
	blockSize := int32(16)
	volumeSize := int64(32) // 2 blocks

	data := make([]byte, volumeSize)
	for i := 0; i < len(data); i++ {
		data[i] = 0xDD
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
	}

	sr, err := Open("snap-test", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	// Read beyond EOF: offset at volumeSize
	buf := make([]byte, 8)
	n, err := sr.ReadAt(buf, volumeSize)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected n=0, got %d", n)
	}

	// Read that extends past EOF
	buf = make([]byte, 24)
	n, err = sr.ReadAt(buf, int64(blockSize))
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != int(blockSize) {
		t.Fatalf("expected n=%d, got %d", blockSize, n)
	}
	for i := 0; i < int(blockSize); i++ {
		if buf[i] != 0xDD {
			t.Errorf("buf[%d] = 0x%02X, want 0xDD", i, buf[i])
		}
	}
}

func newLatencyMock(latency time.Duration) *MockEBS {
	const blockSize = int32(16)
	return &MockEBS{
		Reader:     bytes.NewReader(make([]byte, blockSize)),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(int64(blockSize)),
		Latency:    latency,
	}
}

func TestMockEBS_GetSnapshotBlockCount(t *testing.T) {
	mock := newLatencyMock(0)
	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}

	if got := mock.GetSnapshotBlockCount(); got != 0 {
		t.Fatalf("initial count = %d, want 0", got)
	}
	for i := 0; i < 3; i++ {
		if _, err := mock.GetSnapshotBlock(context.Background(), params); err != nil {
			t.Fatal(err)
		}
	}
	if got := mock.GetSnapshotBlockCount(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
}

func TestMockEBS_GetSnapshotBlockCount_Concurrent(t *testing.T) {
	mock := newLatencyMock(0)
	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if _, err := mock.GetSnapshotBlock(context.Background(), params); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if got := mock.GetSnapshotBlockCount(); got != goroutines {
		t.Fatalf("count = %d, want %d", got, goroutines)
	}
}

func TestMockEBS_Latency(t *testing.T) {
	const latency = 50 * time.Millisecond
	mock := newLatencyMock(latency)

	start := time.Now()
	if _, err := mock.ListSnapshotBlocks(context.Background(), &ebs.ListSnapshotBlocksInput{}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < latency {
		t.Errorf("ListSnapshotBlocks took %v, want at least %v", elapsed, latency)
	}

	start = time.Now()
	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}
	if _, err := mock.GetSnapshotBlock(context.Background(), params); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < latency {
		t.Errorf("GetSnapshotBlock took %v, want at least %v", elapsed, latency)
	}
}

func TestMockEBS_LatencyUnset(t *testing.T) {
	mock := newLatencyMock(0)
	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}

	start := time.Now()
	if _, err := mock.GetSnapshotBlock(context.Background(), params); err != nil {
		t.Fatal(err)
	}
	// Loose bound so that GC or scheduler pauses under -race cannot fail the test.
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("GetSnapshotBlock took %v, want no delay", elapsed)
	}
}

func TestMockEBS_GetSnapshotBlockCount_ReadAt(t *testing.T) {
	blockSize := int32(16)
	numBlocks := int32(4)
	mock := &MockEBS{
		Reader:     bytes.NewReader(make([]byte, blockSize*numBlocks)),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(int64(blockSize * numBlocks)),
		Allocated:  map[int32]bool{0: true, 1: true, 3: true}, // block 2 is unallocated
		Latency:    time.Millisecond,
	}

	// A nil cache never hits, so every allocated block is fetched.
	sr, err := Open("snap-test", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, blockSize*numBlocks)
	if _, err := sr.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if got := mock.GetSnapshotBlockCount(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
}

func TestMockEBS_ContextCanceledBeforeCall(t *testing.T) {
	// Latency is unset: the context is checked even without a delay.
	mock := newLatencyMock(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}
	if _, err := mock.GetSnapshotBlock(ctx, params); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetSnapshotBlock error = %v, want context.Canceled", err)
	}
	if _, err := mock.ListSnapshotBlocks(ctx, &ebs.ListSnapshotBlocksInput{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListSnapshotBlocks error = %v, want context.Canceled", err)
	}

	// The call was issued, so it is counted even though it failed.
	if got := mock.GetSnapshotBlockCount(); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
}

func TestMockEBS_ContextCanceledDuringLatency(t *testing.T) {
	mock := newLatencyMock(30 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	defer cancel()

	start := time.Now()
	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}
	if _, err := mock.GetSnapshotBlock(ctx, params); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetSnapshotBlock error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed >= mock.Latency {
		t.Errorf("GetSnapshotBlock took %v, want it to abort well before %v", elapsed, mock.Latency)
	}
}
