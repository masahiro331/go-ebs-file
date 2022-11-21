package ebsfile

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ebs"
	"github.com/aws/aws-sdk-go/service/ebs/ebsiface"
)

type MockEBS struct {
	ebsiface.EBSAPI

	f          *os.File
	BlockSize  *int64
	NextToken  *string
	VolumeSize *int64
}

func (m MockEBS) WalkSnapshotBlocks(_ context.Context, _ *ebs.ListSnapshotBlocksInput, _ map[int64]string) (*ebs.ListSnapshotBlocksOutput, map[int64]string, error) {
	indexLen := *m.VolumeSize / *m.BlockSize
	var blocks []*ebs.Block
	t := make(map[int64]string)
	for i := int64(0); i < indexLen; i++ {
		blocks = append(blocks, &ebs.Block{BlockIndex: aws.Int64(i)})
		t[i] = ""
	}

	return &ebs.ListSnapshotBlocksOutput{
		Blocks:     blocks,
		BlockSize:  m.BlockSize, // 512 KB
		VolumeSize: m.VolumeSize,
	}, t, nil
}

func (m MockEBS) GetSnapshotBlockWithContext(_ aws.Context, input *ebs.GetSnapshotBlockInput, _ ...request.Option) (*ebs.GetSnapshotBlockOutput, error) {
	buf := make([]byte, *m.BlockSize)

	offset := *input.BlockIndex * (*m.BlockSize)
	n, err := m.f.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return &ebs.GetSnapshotBlockOutput{
		BlockData:  io.NopCloser(bytes.NewReader(buf)),
		DataLength: aws.Int64(int64(n)),
	}, nil
}

func NewMockEBS(filePath string, blockSize, volumeSize int64) EBSAPI {
	f, err := os.Open(filePath)
	if err != nil {
		return nil
	}
	return &MockEBS{
		f:          f,
		BlockSize:  aws.Int64(blockSize),
		VolumeSize: aws.Int64(volumeSize),
	}
}

type EBS struct {
	ebsiface.EBSAPI
}

type EBSAPI interface {
	ebsiface.EBSAPI
	WalkSnapshotBlocks(ctx context.Context, input *ebs.ListSnapshotBlocksInput, table map[int64]string) (*ebs.ListSnapshotBlocksOutput, map[int64]string, error)
}

type File struct {
	snapshotID string
	size       int64
	blockSize  int64
	blockTable map[int64]string

	ebsclient EBSAPI
	cache     Cache
	ctx       context.Context
}

type Option struct {
	AwsSecretKey string
	AwsAccessKey string
	AwsRegion    string
}

func New(option Option) *EBS {
	sess := session.Must(getSession(option))
	return &EBS{ebs.New(sess)}
}

func (e EBS) WalkSnapshotBlocks(ctx context.Context, input *ebs.ListSnapshotBlocksInput, table map[int64]string) (*ebs.ListSnapshotBlocksOutput, map[int64]string, error) {
	output, err := e.ListSnapshotBlocksWithContext(ctx, input)
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
	output, table, err := e.WalkSnapshotBlocks(ctx, input, make(map[int64]string))
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
		if f.blockSize-dataOffset < int64(len(p)) {
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

	if f.blockSize-dataOffset < int64(len(p)) {
		return copy(p, buf[dataOffset:]), nil
	} else {
		return copy(p, buf[dataOffset:dataOffset+int64(len(p))]), nil
	}
}

func (f *File) Size() int64 {
	return f.size
}

func (f *File) translateOffset(offset int64) (int64, int64) {
	index := offset / f.blockSize
	dataOffset := offset % f.blockSize

	return index, dataOffset
}

func (f *File) read(index int64, token string) ([]byte, error) {
	input := &ebs.GetSnapshotBlockInput{
		SnapshotId: &f.snapshotID,
		BlockIndex: &index,
		BlockToken: &token,
	}
	o, err := f.ebsclient.GetSnapshotBlockWithContext(f.ctx, input)
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

func getSession(option Option) (*session.Session, error) {
	// create custom credential information if option is valid
	if option.AwsSecretKey != "" && option.AwsAccessKey != "" && option.AwsRegion != "" {
		return session.NewSessionWithOptions(
			session.Options{
				Config: aws.Config{
					Region: aws.String(option.AwsRegion),
					Credentials: credentials.NewStaticCredentialsFromCreds(
						credentials.Value{
							AccessKeyID:     option.AwsAccessKey,
							SecretAccessKey: option.AwsSecretKey,
						},
					),
				},
			})
	}
	// use shared configuration normally
	return session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
}
