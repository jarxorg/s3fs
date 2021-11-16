// +build integtest

package s3fs

import (
	"os"
	"testing"
	"testing/fstest"
)

func TestFSIntegration(t *testing.T) {
	bucket := os.Getenv("FSTEST_BUCKET")
	expected := os.Getenv("FSTEST_EXPECTED")
	if bucket == "" || expected == "" {
		t.Fatalf("Require ENV FSTEST_BUCKET=%s FSTEST_EXPECTED=%s", bucket, expected)
	}

	fsys := New(bucket)
	if err := fstest.TestFS(fsys, expected); err != nil {
		t.Errorf("Error testing/fstest: %+v", err)
	}
}
