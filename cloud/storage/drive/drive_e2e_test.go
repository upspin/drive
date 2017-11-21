// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package drive

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"upspin.io/cloud/storage"
	"upspin.io/errors"
	"upspin.io/log"
)

var (
	client      storage.Storage
	testDataStr = fmt.Sprintf("This is test at %v", time.Now())
	testData    = []byte(testDataStr)
	fileName    = fmt.Sprintf("test-file-%d", time.Now().Second())

	accessToken  = flag.String("access-token", "", "oauth2 access token")
	refreshToken = flag.String("refresh-token", "", "oauth2 refresh token")
	expiry       = flag.String("expiry", "2017-10-12T09:45:38+02:00", "RFC3999 format time stamp")
	runE2E       = flag.Bool("run-e2e", false, "enable to run tests against an actual Drive account")
)

func TestPutAndDownload(t *testing.T) {
	err := client.Put(fileName, testData)
	if err != nil {
		t.Fatalf("Can't put: %v", err)
	}
	data, err := client.Download(fileName)
	if err != nil {
		t.Fatalf("Can't Download: %v", err)
	}
	if string(data) != testDataStr {
		t.Errorf("Expected %q got %q", testDataStr, string(data))
	}
}

func TestDeleteAndDownload(t *testing.T) {
	err := client.Put(fileName, testData)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Delete(fileName)
	if err != nil {
		t.Fatalf("Expected no errors, got %v", err)
	}
	_, err = client.Download(fileName)
	if err == nil {
		t.Fatal("expected error")
	}
	if e, ok := err.(*errors.Error); !ok || e.Kind != errors.NotExist {
		t.Fatalf("expected NotExist error, got %v", e)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !*runE2E || *accessToken == "" || *refreshToken == "" {
		log.Printf(`

cloud/storage/drive: skipping test as it requires Drive access. To enable this
test, set the -run-e2e flag along with valid -access-token and -refresh-token
flag values.

`)
		os.Exit(0)
	}
	// Set up Drive client.
	var err error
	client, err = storage.Dial("Drive",
		storage.WithKeyValue("accessToken", *accessToken),
		storage.WithKeyValue("refreshToken", *refreshToken),
		storage.WithKeyValue("tokenType", "Bearer"),
		storage.WithKeyValue("expiry", *expiry))
	if err != nil {
		log.Fatalf("cloud/storage/drive: couldn't set up client: %v", err)
	}

	code := m.Run()

	// Clean up.
	if err := client.(*driveImpl).cleanup(); err != nil {
		log.Printf("cloud/storage/drive: clean-up failed: %v", err)
	}
	os.Exit(code)
}

// cleanup removes all files that are prefixed with 'test-file-' from the Drive and
// returns the last error, if any.
func (d *driveImpl) cleanup() error {
	q := "name contains 'test-file-'"
	call := d.files.List().Spaces("appDataFolder").Q(q).Fields("files(id, name)")
	r, err := call.Do()
	if err != nil {
		return err
	}
	var er error
	for _, f := range r.Files {
		if err := d.files.Delete(f.Id).Do(); err != nil {
			er = err
		}
		d.ids.Remove(f.Name)
	}
	return er
}
