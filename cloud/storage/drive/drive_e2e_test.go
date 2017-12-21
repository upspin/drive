// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package drive

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"upspin.io/cloud/storage"
	"upspin.io/errors"
	"upspin.io/log"
)

const testFilePrefix = "Upspin-Storage-Test"

var (
	client   storage.Storage
	testData = []byte(fmt.Sprintf("Upspin storage test at %v", time.Now()))
	fileName = fmt.Sprintf("%s%d", testFilePrefix, time.Now().Second())

	accessToken  = flag.String("access-token", "", "oauth2 access token")
	refreshToken = flag.String("refresh-token", "", "oauth2 refresh token")
	expiry       = flag.String("expiry", time.Now().Format(time.RFC3339), "RFC3339 format time stamp")
	runE2E       = flag.Bool("run-e2e", false, "enable to run tests against an actual Drive account")
)

func TestPutAndDownload(t *testing.T) {
	err := client.Put(fileName, testData)
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Download(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, testData) {
		t.Errorf("expected %q got %q", testData, got)
	}
}

func TestList(t *testing.T) {
	err := client.Put(fileName, testData)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := client.(storage.Lister).List("")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("expected at least one result")
	}
}

func TestDeleteAndDownload(t *testing.T) {
	err := client.Put(fileName, testData)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Delete(fileName)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Download(fileName)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(errors.NotExist, err) {
		t.Fatalf("expected NotExist error, got %v", err)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !*runE2E || *accessToken == "" || *refreshToken == "" {
		log.Printf(`

cloud/storage/drive: skipping test as it requires Google Drive credentials. To enable this
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
	client.(*driveImpl).cleanup()
	os.Exit(code)
}

// cleanup removes all files with the prefix testFilePrefix from Google Drive.
func (d *driveImpl) cleanup() {
	q := fmt.Sprintf("name contains %q", testFilePrefix)
	call := d.files.List().Spaces("appDataFolder").Q(q).Fields("files(id, name)")
	r, err := call.Do()
	if err != nil {
		log.Fatalf("cleanup: %v", err)
	}
	for _, f := range r.Files {
		if err := d.files.Delete(f.Id).Do(); err != nil {
			log.Fatalf("cleanup: %v", err)
		}
		d.ids.Remove(f.Name)
	}
}
