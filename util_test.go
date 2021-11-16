package s3fs

import (
	"io/fs"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestIsNotExist(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{
			err:  fs.ErrNotExist,
			want: true,
		}, {
			err:  &fs.PathError{Err: fs.ErrNotExist},
			want: true,
		}, {
			err:  fs.ErrExist,
			want: false,
		},
	}
	for _, test := range tests {
		got := isNotExist(test.err)
		if got != test.want {
			t.Errorf(`Error isNotExist(%v) returns %v; want %v`, test.err, got, test.want)
		}
	}
}

func TestIsS3NoSuchKey(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{
			err:  awserr.New(s3.ErrCodeNoSuchKey, "", nil),
			want: true,
		}, {
			err:  fs.ErrNotExist,
			want: false,
		},
	}
	for _, test := range tests {
		got := isS3NoSuchKey(test.err)
		if got != test.want {
			t.Errorf(`Error isS3NoSuchKey(%v) returns %v; want %v`, test.err, got, test.want)
		}
	}
}

func TestToPathError(t *testing.T) {
	op := "open"
	name := "test.txt"

	tests := []struct {
		err  error
		want error
	}{
		{
			err:  awserr.New(s3.ErrCodeNoSuchKey, "", nil),
			want: &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist},
		}, {
			err:  fs.ErrNotExist,
			want: &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist},
		}, {
			err:  fs.ErrExist,
			want: &fs.PathError{Op: op, Path: name, Err: fs.ErrExist},
		},
	}
	for _, test := range tests {
		got := toPathError(test.err, op, name)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf(`Error toPathError(%v) returns %v; want %v`, test.err, got, test.want)
		}
	}
}

func TestToS3NoSuckKeyIfNoExist(t *testing.T) {
	tests := []struct {
		err  error
		want error
	}{
		{
			err:  fs.ErrNotExist,
			want: awserr.New(s3.ErrCodeNoSuchKey, "", nil),
		}, {
			err:  &fs.PathError{Err: fs.ErrNotExist},
			want: awserr.New(s3.ErrCodeNoSuchKey, "", nil),
		}, {
			err:  fs.ErrExist,
			want: fs.ErrExist,
		},
	}
	for _, test := range tests {
		got := toS3NoSuckKeyIfNoExist(test.err)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf(`Error toS3NoSuckKeyIfNoExist(%v) returns %v; want %v`, test.err, got, test.want)
		}
	}
}

func TestNormalizePrefix(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{
			prefix: ".",
			want:   "",
		}, {
			prefix: "/.",
			want:   "",
		}, {
			prefix: "dir",
			want:   "dir/",
		}, {
			prefix: "dir/",
			want:   "dir/",
		}, {
			prefix: "dir/.",
			want:   "dir/",
		},
	}
	for _, test := range tests {
		got := normalizePrefix(test.prefix)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf(`Error normalizePrefix(%s) returns %s; want %s`, test.prefix, got, test.want)
		}
	}
}
