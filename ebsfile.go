package ebsfile

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ebs"
	"github.com/aws/aws-sdk-go-v2/service/ebs/types"
)

type MockEBS struct {
	EBSAPI

	f          *os.File
	BlockSize  *int32
	NextToken  *string
	VolumeSize *int64
}

func (m MockEBS) WalkSnapshotBlocks(_ context.Context, _ *ebs.ListSnapshotBlocksInput, _ map[int32]string) (*ebs.ListSnapshotBlocksOutput, map[int32]string, error) {
	indexLen := *m.VolumeSize / int64(*m.BlockSize)
	var blocks []types.Block
	t := make(map[int32]string)
	for i := int32(0); i < int32(indexLen); i++ {
		blocks = append(blocks, types.Block{BlockIndex: aws.Int32(i)})
		t[i] = ""
	}

	return &ebs.ListSnapshotBlocksOutput{
		Blocks:     blocks,
		BlockSize:  m.BlockSize, // 512 KB
		VolumeSize: m.VolumeSize,
	}, t, nil
}

func (m MockEBS) GetSnapshotBlock(_ context.Context, params *ebs.GetSnapshotBlockInput, optFns ...func(*ebs.Options)) (*ebs.GetSnapshotBlockOutput, error) {
	buf := make([]byte, *m.BlockSize)

	offset := *params.BlockIndex * (*m.BlockSize)
	n, err := m.f.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}
	return &ebs.GetSnapshotBlockOutput{
		BlockData:  io.NopCloser(bytes.NewReader(buf)),
		DataLength: aws.Int32(int32(n)),
	}, nil
}

func NewMockEBS(filePath string, blockSize int32, volumeSize int64) EBSAPI {
	f, err := os.Open(filePath)
	if err != nil {
		return nil
	}
	return &MockEBS{
		f:          f,
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
	}
}

type EBS struct {
	*ebs.Client
}

type EBSAPI interface {
	ListSnapshotBlocks(ctx context.Context, params *ebs.ListSnapshotBlocksInput, optFns ...func(*ebs.Options)) (*ebs.ListSnapshotBlocksOutput, error)
	GetSnapshotBlock(ctx context.Context, params *ebs.GetSnapshotBlockInput, optFns ...func(*ebs.Options)) (*ebs.GetSnapshotBlockOutput, error)
	WalkSnapshotBlocks(ctx context.Context, input *ebs.ListSnapshotBlocksInput, table map[int32]string) (*ebs.ListSnapshotBlocksOutput, map[int32]string, error)
}

type File struct {
	snapshotID string
	size       int64
	blockSize  int32
	blockTable map[int32]string

	ebsclient EBSAPI
	cache     Cache
	ctx       context.Context
}

type Option struct {
	AwsSecretKey string
	AwsAccessKey string
	AwsRegion    string
}

func New(option Option) (*EBS, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	if option.AwsRegion != "" {
		cfg.Region = option.AwsRegion
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

func Open(snapID string, ctx context.Context, cache Cache, e EBSAPI) (*io.SectionReader, error) {
	input := &ebs.ListSnapshotBlocksInput{
		SnapshotId: &snapID,
	}
	output, table, err := e.WalkSnapshotBlocks(ctx, input, make(map[int32]string))
	if err != nil {
		return nil, err
	}
	if cache == nil {
		cache = &mockCache{}
	}

	f := &File{
		size:       *output.VolumeSize,
		snapshotID: snapID,
		blockSize:  *output.BlockSize,
		blockTable: table,
		cache:      cache,
		ebsclient:  e,
		ctx:        ctx,
	}
	return io.NewSectionReader(f, 0, f.Size()), nil
}

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	index, dataOffset := f.translateOffset(off)
	token, ok := f.blockTable[index]
	if !ok {
		if f.blockSize-dataOffset < int32(len(p)) {
			return int(f.blockSize - dataOffset), nil
		} else {
			return len(p), nil
		}
	}

	var buf []byte
	bi, ok := f.cache.Get(cacheKey(index))
	if ok {
		buf = bi.([]byte)
	} else {
		buf, err = f.read(index, token)
		if err != nil {
			return 0, err
		}
		f.cache.Add(cacheKey(index), buf)
	}
	if len(buf) != int(f.blockSize) {
		return 0, fmt.Errorf("invalid block size: len(buf): %d != f.blockSize: %d", len(buf), f.blockSize)
	}

	if f.blockSize-dataOffset < int32(len(p)) {
		return copy(p, buf[dataOffset:]), nil
	} else {
		return copy(p, buf[dataOffset:dataOffset+int32(len(p))]), nil
	}
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
	input := &ebs.GetSnapshotBlockInput{
		SnapshotId: &f.snapshotID,
		BlockIndex: &index,
		BlockToken: &token,
	}
	o, err := f.ebsclient.GetSnapshotBlock(f.ctx, input)
	if err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(o.BlockData)
	if err != nil {
		return nil, err
	}
	defer o.BlockData.Close()

	return buf, nil
}
