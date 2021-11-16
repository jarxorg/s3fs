package s3fs

import (
	"bytes"
	"io"
	"io/fs"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jarxorg/io2"
)

type s3File struct {
	*content
	buf io.ReadCloser
}

var (
	_ fs.File     = (*s3File)(nil)
	_ fs.FileInfo = (*s3File)(nil)
)

func newS3File(key string, o *s3.GetObjectOutput) *s3File {
	return &s3File{
		content: &content{
			name:    path.Base(key),
			size:    aws.Int64Value(o.ContentLength),
			modTime: aws.TimeValue(o.LastModified),
		},
		buf: o.Body,
	}
}

// Read reads bytes from this file.
func (f *s3File) Read(p []byte) (int, error) {
	return f.buf.Read(p)
}

// Stat returns the fs.FileInfo of this file.
func (f *s3File) Stat() (fs.FileInfo, error) {
	return f, nil
}

// Close closes streams.
func (f *s3File) Close() error {
	return f.buf.Close()
}

type s3WriterFile struct {
	*content
	fsys  *S3FS
	key   string
	buf   *bytes.Buffer
	wrote bool
}

var (
	_ io2.WriterFile = (*s3WriterFile)(nil)
	_ fs.FileInfo    = (*s3WriterFile)(nil)
)

func newS3WriterFile(fsys *S3FS, key string) *s3WriterFile {
	return &s3WriterFile{
		content: &content{
			name: path.Base(key),
		},
		key: key,
		buf: new(bytes.Buffer),
	}
}

// Write writes the specified bytes to this file.
func (f *s3WriterFile) Write(p []byte) (int, error) {
	if f.buf == nil {
		return 0, toPathError(fs.ErrClosed, "Write", f.key)
	}
	f.wrote = true
	return f.buf.Write(p)
}

// Close closes streams.
func (f *s3WriterFile) Close() error {
	if !f.wrote {
		return nil
	}
	if f.buf == nil {
		return toPathError(fs.ErrClosed, "Close", f.key)
	}
	input := &s3.PutObjectInput{
		Bucket: aws.String(f.fsys.bucket),
		Key:    aws.String(f.fsys.key(f.key)),
		Body:   bytes.NewReader(f.buf.Bytes()),
	}
	f.buf = nil
	var err error
	_, err = f.fsys.api.PutObject(input)
	return err
}

// Read reads bytes from this file.
func (f *s3WriterFile) Read(p []byte) (int, error) {
	if f.buf == nil {
		return 0, &fs.PathError{Op: "Read", Path: f.key, Err: fs.ErrClosed}
	}
	return f.buf.Read(p)
}

// Stat returns the fs.FileInfo of this file.
func (f *s3WriterFile) Stat() (fs.FileInfo, error) {
	return f, nil
}
