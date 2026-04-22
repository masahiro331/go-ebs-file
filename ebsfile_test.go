package ebsfile

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
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
