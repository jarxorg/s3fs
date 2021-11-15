package s3fs

import (
  "errors"
  "io/fs"
  "path"
  "strings"

  "github.com/aws/aws-sdk-go/aws/awserr"
  "github.com/aws/aws-sdk-go/service/s3"
)

func isPathErrorNotExist(err error) bool {
  if err == fs.ErrNotExist {
    return true
  }
  var pathErr *fs.PathError
  return errors.As(err, &pathErr) && pathErr.Err == fs.ErrNotExist
}

func isS3NoSuchKey(err error) bool {
  var awsErr awserr.Error
  return errors.As(err, &awsErr) && awsErr.Code() == s3.ErrCodeNoSuchKey
}

func toPathError(err error, op, name string) error {
  if isS3NoSuchKey(err) {
    err = fs.ErrNotExist
  }
  return  &fs.PathError{Op: op, Path: name, Err: err}
}

func toS3NoSuckKeyIfNoExist(err error) error {
  if isPathErrorNotExist(err) {
    return awserr.New(s3.ErrCodeNoSuchKey, "", nil)
  }
  return err
}

func normalizePrefix(prefix string) string {
  prefix = path.Clean(prefix)
  if prefix == "." {
		prefix = ""
	}
  if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
  return prefix
}
