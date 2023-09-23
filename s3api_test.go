package s3fs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jarxorg/wfs"
)

func TestGetObject(t *testing.T) {
	fsys := newMemFSTesting(t)
	f, err := fsys.Open("testdata/dir0/file01.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	info, err := fs.Stat(fsys, "testdata/dir0/file01.txt")
	if err != nil {
		t.Fatal(err)
	}
	want, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	api := NewFSS3API(fsys)
	input := &s3.GetObjectInput{
		Bucket: aws.String("testdata"),
		Key:    aws.String("dir0/file01.txt"),
	}
	output, err := api.GetObject(input)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Body.Close()

	got, err := io.ReadAll(output.Body)
	if err != nil {
		t.Fatal(err)
	}

	if string(got) != string(want) {
		t.Errorf(`Error Body %s; want %s`, got, want)
	}
	if aws.Int64Value(output.ContentLength) != info.Size() {
		t.Errorf(`Error ContentLength %d; want %d`, output.ContentLength, info.Size())
	}
}

func TestGetObject_OutputBodyReadError(t *testing.T) {
	wantErr := errors.New("test")
	fsys := wfs.DelegateFS(newMemFSTesting(t))
	fsys.OpenFunc = func(name string) (fs.File, error) {
		return nil, wantErr
	}

	api := NewFSS3API(fsys)
	input := &s3.GetObjectInput{
		Bucket: aws.String("testdata"),
		Key:    aws.String("dir0/file01.txt"),
	}
	output, err := api.GetObject(input)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Body.Close()

	_, gotErr := output.Body.Read([]byte{})
	if gotErr != wantErr {
		t.Errorf(`Error Read error got %v; want %v`, gotErr, wantErr)
	}
}

func TestGetObject_OutputBodyClose(t *testing.T) {
	fsys := newMemFSTesting(t)

	api := NewFSS3API(fsys)
	input := &s3.GetObjectInput{
		Bucket: aws.String("testdata"),
		Key:    aws.String("dir0/file01.txt"),
	}
	output, err := api.GetObject(input)
	if err != nil {
		t.Fatal(err)
	}
	err = output.Body.Close()
	if err != nil {
		t.Errorf(`Error %v on closing output body`, err)
	}
}

func TestGetObject_StatError(t *testing.T) {
	wantErr := errors.New("test")
	fsys := wfs.DelegateFS(newMemFSTesting(t))
	fsys.StatFunc = func(name string) (fs.FileInfo, error) {
		return nil, wantErr
	}

	api := NewFSS3API(fsys)
	input := &s3.GetObjectInput{
		Bucket: aws.String("testdata"),
		Key:    aws.String("dir0/file01.txt"),
	}
	_, gotErr := api.GetObject(input)
	if gotErr != wantErr {
		t.Errorf(`Error GetObject error got %v; want %v`, gotErr, wantErr)
	}
}

func TestGetObject_DirError(t *testing.T) {
	fsys := newMemFSTesting(t)

	api := NewFSS3API(fsys)
	input := &s3.GetObjectInput{
		Bucket: aws.String("testdata"),
		Key:    aws.String("dir0"),
	}
	wantErr := toS3NoSuckKeyIfNoExist(fs.ErrNotExist)
	_, gotErr := api.GetObject(input)
	if !reflect.DeepEqual(gotErr, wantErr) {
		t.Errorf(`Error GetObject error got %v; want %v`, gotErr, wantErr)
	}
}

func TestPutObject(t *testing.T) {
	fsys := newMemFSTesting(t)
	want := []byte("test")

	api := NewFSS3API(fsys)
	input := &s3.PutObjectInput{
		Bucket: aws.String("testdata"),
		Key:    aws.String("test.txt"),
		Body:   bytes.NewReader(want),
	}
	_, err := api.PutObject(input)
	if err != nil {
		t.Fatal(err)
	}

	f, err := fsys.Open("testdata/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	got, err := io.ReadAll(f)
	if string(got) != string(want) {
		t.Errorf(`Error PutObject wrote %s; want %s`, got, want)
	}
}

func TestPutObject_CreateFileError(t *testing.T) {
	wantErr := errors.New("test")
	fsys := wfs.DelegateFS(newMemFSTesting(t))
	fsys.CreateFileFunc = func(name string, mode fs.FileMode) (wfs.WriterFile, error) {
		return nil, wantErr
	}

	api := NewFSS3API(fsys)
	input := &s3.PutObjectInput{
		Bucket: aws.String("testdata"),
		Key:    aws.String("test.txt"),
		Body:   bytes.NewReader([]byte{}),
	}
	_, gotErr := api.PutObject(input)
	if gotErr != wantErr {
		t.Errorf(`Error PutObject error got %v; want %v`, gotErr, wantErr)
	}
}

func TestListObjectV2(t *testing.T) {
	fsys := newMemFSTesting(t)
	limit := 1
	after := "dir0/file01.txt"
	want := &s3.ListObjectsV2Output{
		IsTruncated: aws.Bool(true),
	}
	err := fs.WalkDir(fsys, "testdata/dir0", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if len(want.Contents) >= limit {
			return fs.SkipDir
		}
		key := strings.TrimPrefix(p, "testdata/")
		if after >= key {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		want.Contents = append(want.Contents, &s3.Object{
			Key:          aws.String(key),
			Size:         aws.Int64(info.Size()),
			LastModified: aws.Time(info.ModTime()),
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	api := NewFSS3API(fsys)
	input := &s3.ListObjectsV2Input{
		Bucket:     aws.String("testdata"),
		Prefix:     aws.String("dir0"),
		MaxKeys:    aws.Int64(int64(limit)),
		StartAfter: aws.String(after),
	}
	got, err := api.ListObjectsV2(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf(`Error ListObjectsV2 got %v; want %v`, got, want)
	}
}

func TestListObjectV2_DirEntryInfoError(t *testing.T) {
	wantErr := errors.New("test")
	fsys := wfs.DelegateFS(newMemFSTesting(t))
	fsys.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
		return []fs.DirEntry{
			&wfs.DirEntryDelegator{
				InfoFunc: func() (fs.FileInfo, error) {
					return nil, wantErr
				},
			},
		}, nil
	}

	api := NewFSS3API(fsys)
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String("testdata"),
		Prefix:    aws.String("dir0"),
		Delimiter: aws.String("/"),
	}
	_, gotErr := api.ListObjectsV2(input)
	if gotErr != wantErr {
		t.Errorf(`Error ListObjectsV2 error got %v; want %v`, gotErr, wantErr)
	}
}

func TestListObjectV2_Delimiter(t *testing.T) {
	fsys := newMemFSTesting(t)
	limit := 1
	after := "file0.txt"
	want := &s3.ListObjectsV2Output{
		IsTruncated: aws.Bool(true),
	}
	ds, err := fs.ReadDir(fsys, "testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		if d.IsDir() {
			want.CommonPrefixes = append(want.CommonPrefixes, &s3.CommonPrefix{
				Prefix: aws.String(d.Name()),
			})
			continue
		}
		if after >= d.Name() {
			continue
		}
		info, err := d.Info()
		if err != nil {
			t.Fatal(err)
		}
		want.Contents = append(want.Contents, &s3.Object{
			Key:          aws.String(info.Name()),
			Size:         aws.Int64(info.Size()),
			LastModified: aws.Time(info.ModTime()),
		})
		if len(want.Contents) >= limit {
			break
		}
	}

	api := NewFSS3API(fsys)
	input := &s3.ListObjectsV2Input{
		Bucket:     aws.String("testdata"),
		Prefix:     aws.String(""),
		MaxKeys:    aws.Int64(int64(limit)),
		Delimiter:  aws.String("/"),
		StartAfter: aws.String(after),
	}
	got, err := api.ListObjectsV2(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf(`Error ListObjectsV2 got %v; want %v`, got, want)
	}
}

func TestListObjectV2_Delimiter_ReadDirError(t *testing.T) {
	wantErr := errors.New("test")
	fsys := wfs.DelegateFS(newMemFSTesting(t))
	fsys.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
		return nil, wantErr
	}

	api := NewFSS3API(fsys)
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String("testdata"),
		Prefix:    aws.String(""),
		Delimiter: aws.String("/"),
	}
	_, gotErr := api.ListObjectsV2(input)
	if gotErr != wantErr {
		t.Errorf(`Error ListObjectsV2 error got %v; want %v`, gotErr, wantErr)
	}
}

func TestListObjectV2_Delimiter_DirEntryInfoError(t *testing.T) {
	wantErr := errors.New("test")
	fsys := wfs.DelegateFS(newMemFSTesting(t))
	fsys.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
		return []fs.DirEntry{
			&wfs.DirEntryDelegator{
				Values: wfs.DirEntryValues{
					Name: "test",
				},
				InfoFunc: func() (fs.FileInfo, error) {
					return nil, wantErr
				},
			},
		}, nil
	}

	api := NewFSS3API(fsys)
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String("testdata"),
		Prefix:    aws.String(""),
		Delimiter: aws.String("/"),
	}
	_, gotErr := api.ListObjectsV2(input)
	if gotErr != wantErr {
		t.Errorf(`Error ListObjectsV2 error got %v; want %v`, gotErr, wantErr)
	}
}
