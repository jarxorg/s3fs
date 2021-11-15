package s3fs

import (
  "io/fs"
  "testing"
  "time"

  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/service/s3"
)

func TestNewDirContent(t *testing.T) {
  prefix := "dir"

  var got fs.DirEntry
  got = newDirContent(prefix)

  if name := got.Name(); name != prefix {
    t.Errorf("Error Name %s; want %s", name, prefix)
  }
  if isDir := got.IsDir(); !isDir {
    t.Errorf("Error IsDir %v; want true", isDir)
  }
  if typ := got.Type(); typ != fs.ModeDir {
    t.Errorf("Error Type %v; want %v", typ, fs.ModeDir)
  }

  info, err := got.Info()
  if err != nil {
    t.Fatal(err)
  }
  if infoName := info.Name(); infoName != prefix {
    t.Errorf("Error FileInfo.Name %s; want %s", infoName, prefix)
  }
  if size := info.Size(); size != 0 {
    t.Errorf("Error FileInfo.Size %d; want 0", size)
  }
  if mode := info.Mode(); mode != fs.ModePerm | fs.ModeDir {
    t.Errorf("Error FileInfo.Mode %v; want %v", mode, fs.ModePerm | fs.ModeDir)
  }
  if modTime := info.ModTime(); modTime != (time.Time{}) {
    t.Errorf("Error FileInfo.ModTime %v; want %v", modTime, time.Time{})
  }
  if isDir := info.IsDir(); !isDir {
    t.Errorf("Error FileInfo.IsDir %v; want true", isDir)
  }
  if sys := info.Sys(); sys != nil {
    t.Errorf("Error FileInfo.Sys %v; want nil", sys)
  }
}

func TestNewFileContent(t *testing.T) {
  o := &s3.Object{
    Key: aws.String("file"),
    Size: aws.Int64(123),
    LastModified: aws.Time(time.Now()),
  }

  var got fs.FileInfo
  got = newFileContent(o)

  if name := got.Name(); name != aws.StringValue(o.Key) {
    t.Errorf("Error Name %s; want %s", name, aws.StringValue(o.Key))
  }
  if size := got.Size(); size != aws.Int64Value(o.Size) {
    t.Errorf("Error Size %d; want %d", size, aws.Int64Value(o.Size))
  }
  if mode := got.Mode(); mode != fs.ModePerm {
    t.Errorf("Error Mode %v; want %v", mode, fs.ModePerm)
  }
  if modTime := got.ModTime(); modTime != aws.TimeValue(o.LastModified) {
    t.Errorf("Error ModTime %v; want %v", modTime, aws.TimeValue(o.LastModified))
  }
  if isDir := got.IsDir(); isDir {
    t.Errorf("Error IsDir %v; want false", isDir)
  }
  if sys := got.Sys(); sys != nil {
    t.Errorf("Error Sys %v; want nil", sys)
  }
}
