# github.com/jarxorg/s3fs

[![PkgGoDev](https://pkg.go.dev/badge/github.com/jarxorg/s3fs)](https://pkg.go.dev/github.com/jarxorg/s3fs)
[![Report Card](https://goreportcard.com/badge/github.com/jarxorg/s3fs)](https://goreportcard.com/report/github.com/jarxorg/s3fs)

Package s3fs provides an implementation of [fs2](https://pkg.go.dev/github.com/jarxorg/fs2) for S3.

## Examples

### ReadDir

```go
package main

import (
  "fmt"
  "io/fs"
  "log"

  "github.com/jarxorg/s3fs"
)

func main() {
  fsys := s3fs.New("<your-bucket>")
  entries, err := fs.ReadDir(fsys, ".")
  if err != nil {
    log.Fatal(err)
  }
  for _, entry := range entries {
    fmt.Println(entry.Name())
  }
}
```

### WriteFile

```go
package main

import (
  "io/fs"
  "log"

  "github.com/jarxorg/fs2"
  "github.com/jarxorg/s3fs"
)

func main() {
  fsys := s3fs.New("<your-bucket>")
  _, err := fs2.WriteFile(fsys, "test.txt", []byte(`Hello`), fs.ModePerm)
  if err != nil {
    log.Fatal(err)
  }
}
```

## Tests

S3FS can pass TestFS in "testing/fstest".

```go
import (
  "testing/fstest"
  "github.com/jarxorg/s3fs"
)

// ...

fsys := s3fs.New("<your-bucket>")
if err := fstest.TestFS(fsys, "<your-expected>"); err != nil {
  t.Errorf("Error testing/fstest: %+v", err)
}
```

## Integration tests

```sh
FSTEST_BUCKET=<Your Bucket> FSTEST_EXPECTED=<Your Directory> go test -tags integtest ./...
```
