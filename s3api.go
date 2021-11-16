package s3fs

import (
	"io"
	"io/fs"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jarxorg/io2"
)

const defaultMaxKeys = int64(1000)

func getMaxKeys(n *int64) int64 {
	i := aws.Int64Value(n)
	if i <= 0 {
		return defaultMaxKeys
	}
	return i
}

// FSS3API provides a simple implementation for mocking on test of s3fs package.
type FSS3API struct {
	s3iface.S3API
	fsys fs.FS
}

var _ s3iface.S3API = (*FSS3API)(nil)

// NewFSS3API returns a s3iface.S3API implementation on the provided filesystem.
func NewFSS3API(fsys fs.FS) *FSS3API {
	return &FSS3API{
		fsys: fsys,
	}
}

// GetObject API operation for the filesystem.
func (api *FSS3API) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	name := path.Join(aws.StringValue(input.Bucket), aws.StringValue(input.Key))
	info, err := fs.Stat(api.fsys, name)
	if err != nil {
		return nil, toS3NoSuckKeyIfNoExist(err)
	}
	if info.IsDir() {
		return nil, toS3NoSuckKeyIfNoExist(fs.ErrNotExist)
	}

	var in io.ReadCloser
	body := &io2.Delegator{}
	body.ReadFunc = func(p []byte) (int, error) {
		if in == nil {
			var err error
			in, err = api.fsys.Open(name)
			if err != nil {
				return 0, err
			}
		}
		return in.Read(p)
	}
	body.CloseFunc = func() error {
		if in != nil {
			return in.Close()
		}
		return nil
	}

	return &s3.GetObjectOutput{
		Body:          body,
		ContentLength: aws.Int64(info.Size()),
		LastModified:  aws.Time(info.ModTime()),
	}, nil
}

// PutObject API operation for the filesystem.
func (api *FSS3API) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	name := path.Join(aws.StringValue(input.Bucket), aws.StringValue(input.Key))
	output := &s3.PutObjectOutput{}
	f, err := io2.CreateFile(api.fsys, name, fs.ModePerm)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	io.Copy(f, input.Body)
	return output, nil
}

func (api *FSS3API) readDir(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	dir := path.Join(aws.StringValue(input.Bucket), aws.StringValue(input.Prefix))
	entries, err := fs.ReadDir(api.fsys, dir)
	if err != nil {
		return nil, toS3NoSuckKeyIfNoExist(err)
	}

	output := &s3.ListObjectsV2Output{}
	limit := getMaxKeys(input.MaxKeys)
	after := aws.StringValue(input.StartAfter)
	limited := false
	truncated := false

	for _, entry := range entries {
		name := path.Join(aws.StringValue(input.Prefix), entry.Name())
		if entry.IsDir() {
			output.CommonPrefixes = append(output.CommonPrefixes, &s3.CommonPrefix{
				Prefix: aws.String(name),
			})
			continue
		}
		if limited {
			truncated = true
			continue
		}
		if after >= name {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, toS3NoSuckKeyIfNoExist(err)
		}
		output.Contents = append(output.Contents, &s3.Object{
			Key:          aws.String(name),
			Size:         aws.Int64(info.Size()),
			LastModified: aws.Time(info.ModTime()),
		})
		limited = (int64(len(output.Contents)) >= limit)
	}

	output.IsTruncated = aws.Bool(truncated)
	return output, nil
}

func (api *FSS3API) walkDir(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	dir := path.Join(aws.StringValue(input.Bucket), aws.StringValue(input.Prefix))
	output := &s3.ListObjectsV2Output{}
	limit := getMaxKeys(input.MaxKeys)
	after := aws.StringValue(input.StartAfter)
	limited := false
	truncated := false

	err := fs.WalkDir(api.fsys, dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if limited {
			truncated = true
			return fs.SkipDir
		}
		name := strings.TrimPrefix(p, aws.StringValue(input.Bucket) + "/")
		if after >= name {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return toS3NoSuckKeyIfNoExist(err)
		}
		output.Contents = append(output.Contents, &s3.Object{
			Key:          aws.String(name),
			Size:         aws.Int64(info.Size()),
			LastModified: aws.Time(info.ModTime()),
		})
		limited = (int64(len(output.Contents)) >= limit)
		return nil
	})
	if err != nil {
		return nil, err
	}

	output.IsTruncated = aws.Bool(truncated)
	return output, nil
}

// ListObjectsV2 API operation for the filesystem.
func (api *FSS3API) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	if aws.StringValue(input.Delimiter) == "/" {
		return api.readDir(input)
	}
	return api.walkDir(input)
}
