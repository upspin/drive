// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package drive implements a storage.Storage that stores data in Google Drive.
package drive // import "drive.upspin.io/cloud/storage/drive"

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"drive.upspin.io/config"

	"upspin.io/cache"
	"upspin.io/cloud/storage"
	"upspin.io/errors"
	"upspin.io/upspin"

	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

func init() {
	storage.Register("Drive", New)
}

// lruSize holds the maximum number of entries that should live in the LRU cache.
// Since it only maps file names to file IDs, 1024 should be affordable to any server.
const lruSize = 1024

// New initializes a new Storage which stores data to Google Drive.
func New(o *storage.Opts) (storage.Storage, error) {
	const op = "cloud/storage/drive.New"
	var (
		t   oauth2.Token
		ok  bool
		err error
	)
	t.AccessToken, ok = o.Opts["accessToken"]
	if !ok {
		return nil, errors.E(op, errors.Invalid, errors.Str("missing accessToken"))
	}
	t.TokenType, ok = o.Opts["tokenType"]
	if !ok {
		return nil, errors.E(op, errors.Invalid, errors.Str("missing tokenType"))
	}
	t.RefreshToken, ok = o.Opts["refreshToken"]
	if !ok {
		return nil, errors.E(op, errors.Invalid, errors.Str("missing refreshToken"))
	}
	t.Expiry, err = time.Parse(time.RFC3339, o.Opts["expiry"])
	if err != nil {
		return nil, errors.E(op, errors.Invalid, errors.Errorf("invalid expiry %q: %v", o.Opts["expiry"], err))
	}
	ctx := context.Background()
	client := config.OAuth2.Client(ctx, &t)
	svc, err := drive.New(client)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	return &driveImpl{
		files: svc.Files,
		ids:   cache.NewLRU(lruSize),
	}, nil
}

var (
	_ storage.Storage = (*driveImpl)(nil)
	_ storage.Lister  = (*driveImpl)(nil)
)

// driveImpl is an implementation of Storage that connects to a Google Drive backend.
type driveImpl struct {
	// files holds the FilesService used to interact with the Drive API.
	files *drive.FilesService
	// ids will map file names to file IDs to avoid hitting the HTTP API
	// twice on each download.
	ids *cache.LRU
}

func (d *driveImpl) LinkBase() (string, error) {
	// Drive does have a LinkBase but it expects it to be followed by the file ID,
	// not by the name of the file. Since we can not use the 'ref' as the file ID
	// this service is not available.
	return "", upspin.ErrNotSupported
}

func (d *driveImpl) Download(ref string) ([]byte, error) {
	const op = "cloud/storage/drive.Download"
	id, err := d.fileId(ref)
	if os.IsNotExist(err) {
		return nil, errors.E(op, errors.NotExist, err)
	}
	if err != nil {
		return nil, errors.E(op, errors.IO, err)
	}
	resp, err := d.files.Get(id).Download()
	if err != nil {
		return nil, errors.E(op, errors.IO, err)
	}
	slurp, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, errors.E(op, errors.IO, err)
	}
	return slurp, nil
}

func (d *driveImpl) Put(ref string, contents []byte) error {
	const op = "cloud/storage/drive.Put"
	// Test whether file exists.
	id, err := d.fileId(ref)
	if err != nil && !os.IsNotExist(err) {
		return errors.E(op, errors.IO, err)
	}
	if id != "" {
		// The file exists. Delete it to ensure uniqueness because Google Drive allows
		// multiple files with the same name to coexist in the same folder. See:
		// https://developers.google.com/drive/v3/reference/files#properties
		if err := d.Delete(ref); err != nil {
			return errors.E(op, err)
		}
	}
	create := d.files.Create(&drive.File{
		Name:    ref,
		Parents: []string{"appDataFolder"},
	})
	contentType := googleapi.ContentType("application/octet-stream")
	_, err = create.Media(bytes.NewReader(contents), contentType).Do()
	if err != nil {
		return errors.E(op, errors.IO, err)
	}
	return nil
}

func (d *driveImpl) Delete(ref string) error {
	const op = "cloud/storage/drive.Download"
	id, err := d.fileId(ref)
	if os.IsNotExist(err) {
		// nothing to delete
		return nil
	}
	if err != nil {
		return errors.E(op, errors.IO, err)
	}
	if err := d.files.Delete(id).Do(); err != nil {
		return errors.E(op, errors.IO, err)
	}
	d.ids.Remove(ref)
	return nil
}

// List implements storage.Lister.
func (d *driveImpl) List(token string) (refs []upspin.ListRefsItem, nextToken string, err error) {
	list := d.files.List().Spaces("appDataFolder").Fields("files(id, quotaBytesUsed)")
	if token != "" {
		list = list.PageToken(token)
	}
	r, err := list.Do()
	if err != nil {
		return nil, "", err
	}
	refs = make([]upspin.ListRefsItem, len(r.Files))
	for i, f := range r.Files {
		refs[i] = upspin.ListRefsItem{
			Ref:  upspin.Reference(f.Id),
			Size: f.QuotaBytesUsed,
		}
	}
	return refs, r.NextPageToken, nil
}

// fileId returns the file ID of the first file found under the given name.
func (d *driveImpl) fileId(name string) (string, error) {
	// try cache first
	if id, ok := d.ids.Get(name); ok {
		return id.(string), nil
	}
	q := fmt.Sprintf(`name=%q`, name)
	list := d.files.List().Spaces("appDataFolder").Q(q).Fields("files(id)")
	r, err := list.Do()
	if err != nil {
		return "", err
	}
	if len(r.Files) == 0 {
		return "", os.ErrNotExist
	}
	id := r.Files[0].Id
	d.ids.Add(name, id)
	return id, nil
}
