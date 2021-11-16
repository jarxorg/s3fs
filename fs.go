package s3fs

import (
	"io/fs"
	"io/ioutil"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jarxorg/io2"
)

const (
	defaultDirOpenBufferSize = 100
	defaultListBufferSize    = 1000
)

// S3FS represents a filesystem on S3 (Amazon Simple Storage Service).
type S3FS struct {
	// DirOpenBufferSize is the buffer size for using objects as the directory. (Default 100)
	DirOpenBufferSize int
	// ListBufferSize is the buffer size for listing objects that is used on
	// ReadDir, Glob and RemoveAll. (Default 1000)
	ListBufferSize int
	api            s3iface.S3API
	bucket         string
	dir            string
}

var (
	_ fs.FS            = (*S3FS)(nil)
	_ fs.GlobFS        = (*S3FS)(nil)
	_ fs.ReadDirFS     = (*S3FS)(nil)
	_ fs.ReadFileFS    = (*S3FS)(nil)
	_ fs.StatFS        = (*S3FS)(nil)
	_ fs.SubFS         = (*S3FS)(nil)
	_ io2.WriteFileFS  = (*S3FS)(nil)
	_ io2.RemoveFileFS = (*S3FS)(nil)
)

// New returns a filesystem for the tree of objects rooted at the specified bucket.
// This function is the same as the following code.
//
//   NewWithSession(bucket, session.Must(
//     session.NewSessionWithOptions(
//       session.Options{SharedConfigState: session.SharedConfigEnable}
//     )
//   ))
func New(bucket string) *S3FS {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	return NewWithSession(bucket, sess)
}

// NewWithSession returns a filesystem for the tree of objects rooted at the specified
// bucket with the session.
func NewWithSession(bucket string, sess *session.Session) *S3FS {
	return NewWithAPI(bucket, s3.New(sess))
}

// NewWithAPI returns a filesystem for the tree of objects rooted at the specified
// bucket with the s3 client.
func NewWithAPI(bucket string, api s3iface.S3API) *S3FS {
	return &S3FS{
		DirOpenBufferSize: defaultDirOpenBufferSize,
		ListBufferSize:    defaultListBufferSize,
		api:               api,
		bucket:            bucket,
	}
}

func (fsys *S3FS) key(name string) string {
	return path.Clean(path.Join(fsys.dir, name))
}

func (fsys *S3FS) rel(name string) string {
	return strings.TrimPrefix(name, normalizePrefix(fsys.dir))
}

func (fsys *S3FS) openFile(name string) (*s3File, error) {
	if !fs.ValidPath(name) {
		return nil, toPathError(fs.ErrInvalid, "Open", name)
	}
	if name == "." || strings.HasSuffix(name, "/.") {
		return nil, toPathError(fs.ErrNotExist, "Open", name)
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(fsys.bucket),
		Key:    aws.String(fsys.key(name)),
	}
	output, err := fsys.api.GetObject(input)
	if err != nil {
		return nil, toPathError(err, "Open", name)
	}
	return newS3File(name, output), nil
}

// Open opens the named file or directory.
func (fsys *S3FS) Open(name string) (fs.File, error) {
	f, err := fsys.openFile(name)
	if err != nil && isNotExist(err) {
		return newS3Dir(fsys, name).open(fsys.DirOpenBufferSize)
	}
	return f, err
}

// ReadDir reads the named directory and returns a list of directory entries
// sorted by filename.
func (fsys *S3FS) ReadDir(dir string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(dir) {
		return nil, toPathError(fs.ErrInvalid, "ReadDir", dir)
	}
	return newS3Dir(fsys, dir).ReadDir(-1)
}

// ReadFile reads the named file and returns its contents.
func (fsys *S3FS) ReadFile(name string) ([]byte, error) {
	f, err := fsys.openFile(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ioutil.ReadAll(f)
}

// Stat returns a FileInfo describing the file. If there is an error, it should be
// of type *PathError.
func (fsys *S3FS) Stat(name string) (fs.FileInfo, error) {
	f, err := fsys.openFile(name)
	if err != nil && isNotExist(err) {
		return newS3Dir(fsys, name).open(1)
	}
	return f, err
}

// Sub returns an FS corresponding to the subtree rooted at dir.
func (fsys *S3FS) Sub(dir string) (fs.FS, error) {
	if !fs.ValidPath(dir) {
		return nil, toPathError(fs.ErrInvalid, "Sub", dir)
	}
	subFsys := NewWithAPI(fsys.bucket, fsys.api)
	subFsys.dir = path.Join(fsys.dir, dir)
	return subFsys, nil
}

// Glob returns the names of all files matching pattern, providing an implementation
// of the top-level Glob function.
func (fsys *S3FS) Glob(pattern string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(fsys.bucket),
		Prefix:  aws.String(normalizePrefix(fsys.dir)),
		MaxKeys: aws.Int64(int64(fsys.ListBufferSize)),
	}

	var keys []string
	matchAppend := func(key string) error {
		ok, err := path.Match(pattern, key)
		if err != nil {
			return toPathError(err, "Glob", pattern)
		}
		if ok {
			keys = append(keys, key)
		}
		return nil
	}

	lastDir := ""
	for {
		output, err := fsys.api.ListObjectsV2(input)
		if err != nil {
			return nil, toPathError(err, "Glob", pattern)
		}
		for _, o := range output.Contents {
			key := fsys.rel(aws.StringValue(o.Key))
			if dir := path.Dir(key); dir != lastDir {
				if err := matchAppend(dir); err != nil {
					return nil, err
				}
				lastDir = dir
			}
			if err := matchAppend(key); err != nil {
				return nil, err
			}
			input.StartAfter = o.Key
		}
		if !*output.IsTruncated {
			break
		}
	}
	return keys, nil
}

// MkdirAll always do nothing.
func (fsys *S3FS) MkdirAll(dir string, mode fs.FileMode) error {
	return nil
}

// CreateFile creates the named file.
// The specified mode is ignored.
func (fsys *S3FS) CreateFile(name string, mode fs.FileMode) (io2.WriterFile, error) {
	if !fs.ValidPath(name) {
		return nil, toPathError(fs.ErrInvalid, "CreateFile", name)
	}
	return newS3WriterFile(fsys, name), nil
}

// WriteFile writes the specified bytes to the named file.
// The specified mode is ignored.
func (fsys *S3FS) WriteFile(name string, p []byte, mode fs.FileMode) (int, error) {
	w := newS3WriterFile(fsys, name)
	n, err := w.Write(p)
	if err != nil {
		return 0, toPathError(err, "Write", name)
	}
	return n, w.Close()
}

// RemoveFile removes the specified named file.
func (fsys *S3FS) RemoveFile(name string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(fsys.bucket),
		Key:    aws.String(fsys.key(name)),
	}
	var err error
	_, err = fsys.api.DeleteObject(input)
	if err != nil {
		return toPathError(err, "RemoveFile", name)
	}
	return nil
}

// RemoveAll removes path and any children it contains.
func (fsys *S3FS) RemoveAll(dir string) error {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(fsys.bucket),
		Prefix:  aws.String(normalizePrefix(fsys.key(dir))),
		MaxKeys: aws.Int64(int64(fsys.ListBufferSize)),
	}
	delInput := &s3.DeleteObjectsInput{
		Bucket: aws.String(fsys.bucket),
		Delete: &s3.Delete{Quiet: aws.Bool(true)},
	}
	for {
		output, err := fsys.api.ListObjectsV2(input)
		if err != nil {
			return toPathError(err, "RemoveAll", dir)
		}
		var ids []*s3.ObjectIdentifier
		for _, o := range output.Contents {
			ids = append(ids, &s3.ObjectIdentifier{Key: o.Key})
			input.StartAfter = o.Key
		}
		delInput.Delete.Objects = ids

		_, err = fsys.api.DeleteObjects(delInput)
		if err != nil {
			return toPathError(err, "RemoveAll", dir)
		}

		if !*output.IsTruncated {
			break
		}
	}
	return nil
}
