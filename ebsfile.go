package ebsfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ebs"
	"github.com/aws/aws-sdk-go-v2/service/ebs/types"
	"golang.org/x/sync/singleflight"
)

var _ EBSAPI = (*MockEBS)(nil)

type MockEBS struct {
	EBSAPI

	Reader     io.ReaderAt
	BlockSize  *int32
	NextToken  *string
	VolumeSize *int64         // volume size in bytes (unlike the real EBS API which returns GiB)
	Allocated  map[int32]bool // nil means all blocks are allocated
	PageSize   int            // 0 means all blocks in a single page (no pagination)
	Latency    time.Duration  // delay added to each ListSnapshotBlocks and GetSnapshotBlock call

	getSnapshotBlockCalls atomic.Int64
}

func (m *MockEBS) wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.Latency <= 0 {
		return nil
	}

	timer := time.NewTimer(m.Latency)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (m *MockEBS) ListSnapshotBlocks(ctx context.Context, params *ebs.ListSnapshotBlocksInput, _ ...func(*ebs.Options)) (*ebs.ListSnapshotBlocksOutput, error) {
	if err := m.wait(ctx); err != nil {
		return nil, err
	}

	numBlocks := int32(*m.VolumeSize / int64(*m.BlockSize))

	startIndex := int32(0)
	if params.NextToken != nil {
		if _, err := fmt.Sscanf(*params.NextToken, "token-%d", &startIndex); err != nil {
			return nil, fmt.Errorf("invalid next token %q: %w", *params.NextToken, err)
		}
	}

	pageSize := m.PageSize
	if pageSize <= 0 {
		pageSize = int(numBlocks)
	}

	var blocks []types.Block
	count := 0
	nextStart := int32(-1)
	for i := startIndex; i < numBlocks; i++ {
		if m.Allocated != nil && !m.Allocated[i] {
			continue
		}
		if count >= pageSize {
			nextStart = i
			break
		}
		blocks = append(blocks, types.Block{
			BlockIndex: aws.Int32(i),
			BlockToken: aws.String(fmt.Sprintf("block-token-%d", i)),
		})
		count++
	}

	output := &ebs.ListSnapshotBlocksOutput{
		Blocks:     blocks,
		BlockSize:  m.BlockSize, // 512 KB
		VolumeSize: m.VolumeSize,
	}
	if nextStart >= 0 {
		output.NextToken = aws.String(fmt.Sprintf("token-%d", nextStart))
	}
	return output, nil
}

func (m *MockEBS) WalkSnapshotBlocks(ctx context.Context, input *ebs.ListSnapshotBlocksInput, table map[int32]string) (*ebs.ListSnapshotBlocksOutput, map[int32]string, error) {
	output, err := m.ListSnapshotBlocks(ctx, input)
	if err != nil {
		return nil, nil, err
	}
	for _, block := range output.Blocks {
		table[*block.BlockIndex] = *block.BlockToken
	}
	if output.NextToken != nil {
		nextInput := *input
		nextInput.NextToken = output.NextToken
		return m.WalkSnapshotBlocks(ctx, &nextInput, table)
	}
	return output, table, nil
}

func (m *MockEBS) GetSnapshotBlock(ctx context.Context, params *ebs.GetSnapshotBlockInput, _ ...func(*ebs.Options)) (*ebs.GetSnapshotBlockOutput, error) {
	m.getSnapshotBlockCalls.Add(1)
	if err := m.wait(ctx); err != nil {
		return nil, err
	}

	buf := make([]byte, *m.BlockSize)

	offset := int64(*params.BlockIndex) * int64(*m.BlockSize)
	n, err := m.Reader.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	sum := sha256.Sum256(buf)
	return &ebs.GetSnapshotBlockOutput{
		BlockData:         io.NopCloser(bytes.NewReader(buf)),
		DataLength:        aws.Int32(int32(n)),
		Checksum:          aws.String(base64.StdEncoding.EncodeToString(sum[:])),
		ChecksumAlgorithm: types.ChecksumAlgorithmChecksumAlgorithmSha256,
	}, nil
}

// GetSnapshotBlockCount returns the number of GetSnapshotBlock calls, including failed ones.
func (m *MockEBS) GetSnapshotBlockCount() int64 {
	return m.getSnapshotBlockCalls.Load()
}

func NewMockEBS(filePath string, blockSize int32, volumeSize int64) EBSAPI {
	f, err := os.Open(filePath)
	if err != nil {
		return nil
	}
	return &MockEBS{
		Reader:     f,
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
	}
}

var _ EBSAPI = (*EBS)(nil)

type EBS struct {
	*ebs.Client
}

type EBSAPI interface {
	ListSnapshotBlocks(ctx context.Context, params *ebs.ListSnapshotBlocksInput, optFns ...func(*ebs.Options)) (*ebs.ListSnapshotBlocksOutput, error)
	GetSnapshotBlock(ctx context.Context, params *ebs.GetSnapshotBlockInput, optFns ...func(*ebs.Options)) (*ebs.GetSnapshotBlockOutput, error)
	WalkSnapshotBlocks(ctx context.Context, input *ebs.ListSnapshotBlocksInput, table map[int32]string) (*ebs.ListSnapshotBlocksOutput, map[int32]string, error)
}

func cacheKey(n int64) string {
	return fmt.Sprintf("ebs:%d", n)
}

type File struct {
	snapshotID string
	size       int64
	blockSize  int32
	blockTable map[int32]string

	ebsclient EBSAPI
	cache     Cache[string, []byte]
	ctx       context.Context
	group     singleflight.Group

	allowMissingChecksum bool
	fetchAttempts        int
}

// DefaultBlockFetchAttempts is the number of times a block is fetched when its
// body cannot be read or verified, unless OpenOptions.BlockFetchAttempts is set.
const DefaultBlockFetchAttempts = 2

// OpenOptions configures OpenWithOptions. The zero value gives the behavior of
// Open.
type OpenOptions struct {
	// AllowMissingChecksum accepts GetSnapshotBlock responses without a
	// checksum. By default such a response is an error. A checksum that does
	// not match the block data is an error regardless of this option.
	AllowMissingChecksum bool

	// BlockFetchAttempts is the maximum number of GetSnapshotBlock calls for a
	// block whose body fails to read, has the wrong length, or does not match
	// its checksum. 1 disables refetching, and a non-positive value uses
	// DefaultBlockFetchAttempts. Errors returned by GetSnapshotBlock itself are
	// not refetched, because the AWS SDK has already retried them.
	BlockFetchAttempts int
}

var (
	errMissingChecksum  = errors.New("response has no checksum; set OpenOptions.AllowMissingChecksum to accept it")
	errChecksumMismatch = errors.New("checksum mismatch")
)

// NewFromConfig creates a new EBS from an existing aws.Config.
func NewFromConfig(cfg aws.Config, optFns ...func(*ebs.Options)) *EBS {
	return &EBS{ebs.NewFromConfig(cfg, optFns...)}
}

func New(ctx context.Context, optFns ...func(*config.LoadOptions) error) (*EBS, error) {
	cfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, err
	}

	ebsClient := ebs.NewFromConfig(cfg)
	return &EBS{ebsClient}, nil
}

func (e EBS) WalkSnapshotBlocks(ctx context.Context, input *ebs.ListSnapshotBlocksInput, table map[int32]string) (*ebs.ListSnapshotBlocksOutput, map[int32]string, error) {
	output, err := e.ListSnapshotBlocks(ctx, input)
	if err != nil {
		return nil, nil, err
	}
	for _, block := range output.Blocks {
		table[*block.BlockIndex] = *block.BlockToken
	}
	if output.NextToken != nil {
		input.NextToken = output.NextToken
		return e.WalkSnapshotBlocks(ctx, input, table)
	}
	output.VolumeSize = aws.Int64(*output.VolumeSize << 30)
	return output, table, nil
}

func Open(snapID string, ctx context.Context, cache Cache[string, []byte], e EBSAPI) (*io.SectionReader, error) {
	return OpenWithOptions(snapID, ctx, cache, e, OpenOptions{})
}

// OpenWithOptions is Open with options. Each block read from the snapshot is
// verified against the SHA256 checksum returned by GetSnapshotBlock before it
// is cached.
func OpenWithOptions(snapID string, ctx context.Context, cache Cache[string, []byte], e EBSAPI, options OpenOptions) (*io.SectionReader, error) {
	if options.BlockFetchAttempts <= 0 {
		options.BlockFetchAttempts = DefaultBlockFetchAttempts
	}
	input := &ebs.ListSnapshotBlocksInput{
		SnapshotId: &snapID,
	}
	output, table, err := e.WalkSnapshotBlocks(ctx, input, make(map[int32]string))
	if err != nil {
		return nil, err
	}
	if cache == nil {
		cache = &mockCache[string, []byte]{}
	}

	f := &File{
		size:       *output.VolumeSize,
		snapshotID: snapID,
		blockSize:  *output.BlockSize,
		blockTable: table,
		cache:      cache,
		ebsclient:  e,
		ctx:        ctx,

		allowMissingChecksum: options.AllowMissingChecksum,
		fetchAttempts:        options.BlockFetchAttempts,
	}
	return io.NewSectionReader(f, 0, f.Size()), nil
}

// ReadAt implements io.ReaderAt. It is safe for concurrent use if the Cache
// passed to Open is. Concurrent reads of the same uncached block share a
// single GetSnapshotBlock request.
func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("ebsfile: negative offset %d", off)
	}
	if off >= f.size {
		return 0, io.EOF
	}

	for n < len(p) && off+int64(n) < f.size {
		index, dataOffset := f.translateOffset(off + int64(n))
		readable := int(f.blockSize - dataOffset)
		remaining := len(p) - n
		if readable > remaining {
			readable = remaining
		}
		if volRemaining := int(f.size - off - int64(n)); readable > volRemaining {
			readable = volRemaining
		}

		token, ok := f.blockTable[index]
		if !ok {
			// Blocks not in blockTable are unallocated in the EBS snapshot,
			// which represent zero-filled disk regions.
			for i := 0; i < readable; i++ {
				p[n+i] = 0
			}
			n += readable
			continue
		}

		var buf []byte
		buf, err = f.loadBlock(index, token)
		if err != nil {
			return n, err
		}
		if len(buf) != int(f.blockSize) {
			return n, fmt.Errorf("invalid block size: len(buf): %d != f.blockSize: %d", len(buf), f.blockSize)
		}

		copied := copy(p[n:], buf[dataOffset:dataOffset+int32(readable)])
		n += copied
	}

	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// loadBlock returns the block at index, coalescing concurrent fetches of it.
func (f *File) loadBlock(index int32, token string) ([]byte, error) {
	key := cacheKey(int64(index))
	if buf, ok := f.cache.Get(key); ok {
		return buf, nil
	}

	v, err, _ := f.group.Do(key, func() (any, error) {
		// Another fetch may have cached the block since the Get above.
		if buf, ok := f.cache.Get(key); ok {
			return buf, nil
		}
		buf, err := f.read(index, token)
		if err != nil {
			return nil, err
		}
		f.cache.Add(key, buf)
		return buf, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

func (f *File) Size() int64 {
	return f.size
}

func (f *File) translateOffset(offset int64) (int32, int32) {
	index := offset / int64(f.blockSize)
	dataOffset := offset % int64(f.blockSize)

	return int32(index), int32(dataOffset)
}

func (f *File) read(index int32, token string) ([]byte, error) {
	for attempt := 1; ; attempt++ {
		buf, retryable, err := f.fetchBlock(index, token)
		if err == nil {
			return buf, nil
		}
		if !retryable || attempt >= f.fetchAttempts || f.ctx.Err() != nil {
			if attempt > 1 {
				return nil, fmt.Errorf("ebsfile: block %d after %d attempts: %w", index, attempt, err)
			}
			return nil, fmt.Errorf("ebsfile: block %d: %w", index, err)
		}
	}
}

// fetchBlock fetches and verifies one block. It reports whether the failure
// may go away on another fetch. A missing checksum or an unsupported algorithm
// will not, so they are not retryable.
func (f *File) fetchBlock(index int32, token string) (buf []byte, retryable bool, err error) {
	input := &ebs.GetSnapshotBlockInput{
		SnapshotId: &f.snapshotID,
		BlockIndex: &index,
		BlockToken: &token,
	}
	o, err := f.ebsclient.GetSnapshotBlock(f.ctx, input)
	if err != nil {
		return nil, false, err
	}
	defer o.BlockData.Close()

	buf, err = io.ReadAll(o.BlockData)
	if err != nil {
		return nil, true, err
	}
	if len(buf) != int(f.blockSize) {
		return nil, true, fmt.Errorf("invalid block size: got %d bytes, want %d", len(buf), f.blockSize)
	}

	checksum := aws.ToString(o.Checksum)
	if checksum == "" {
		if f.allowMissingChecksum {
			return buf, false, nil
		}
		return nil, false, errMissingChecksum
	}
	if o.ChecksumAlgorithm != types.ChecksumAlgorithmChecksumAlgorithmSha256 {
		return nil, false, fmt.Errorf("unsupported checksum algorithm %q", o.ChecksumAlgorithm)
	}
	sum := sha256.Sum256(buf)
	if base64.StdEncoding.EncodeToString(sum[:]) != checksum {
		return nil, true, errChecksumMismatch
	}
	return buf, false, nil
}
