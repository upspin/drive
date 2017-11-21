// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package drive // import "drive.upspin.io/cloud/storage/drive"

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"drive.upspin.io/config"
	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"upspin.io/cache"
	"upspin.io/cloud/storage"
	"upspin.io/errors"
	"upspin.io/upspin"
)

func init() {
	storage.Register("Drive", New)
}

// LRUSize holds the maximum number of entries that should live in the LRU cache.
// Since it only maps file names to file IDs, 60 should be affordable to any server.
const LRUSize = 60

// ErrTokenOpts is returned when options are missing from the storage configuration
var ErrTokenOpts = errors.Errorf("one or more required options are missing, need: accessToken, tokenType, refreshToken, expiry")

// New initializes a new Storage which stores data to Google Drive.
func New(o *storage.Opts) (storage.Storage, error) {
	const op = "cloud/storage/drive.New"
	a, ok := o.Opts["accessToken"]
	if !ok {
		return nil, errors.E(op, errors.Internal, ErrTokenOpts)
	}
	t, ok := o.Opts["tokenType"]
	if !ok {
		return nil, errors.E(op, errors.Internal, ErrTokenOpts)
	}
	r, ok := o.Opts["refreshToken"]
	if !ok {
		return nil, errors.E(op, errors.Internal, ErrTokenOpts)
	}
	e, err := time.Parse(time.RFC3339, o.Opts["expiry"])
	if err != nil {
		return nil, errors.E(op, errors.Internal, errors.Errorf("couldn't parse expiry: ", err))
	}
	ctx := context.Background()
	client := config.OAuth2.Client(ctx, &oauth2.Token{
		AccessToken:  a,
		TokenType:    t,
		RefreshToken: r,
		Expiry:       e,
	})
	svc, err := drive.New(client)
	if err != nil {
		return nil, errors.E(op, errors.Internal, errors.Errorf("unable to retreieve drive client: %v", err))
	}
	return &driveImpl{
		files: svc.Files,
		ids:   cache.NewLRU(LRUSize),
	}, nil
}

var _ storage.Storage = (*driveImpl)(nil)

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
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.E(op, errors.NotExist, err)
		}
		return nil, errors.E(op, errors.IO, err)
	}
	resp, err := d.files.Get(id).Download()
	if err != nil {
		return nil, errors.E(op, errors.IO, err)
	}
	defer resp.Body.Close()
	slurp, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.E(op, errors.IO, err)
	}
	return slurp, nil
}

func (d *driveImpl) Put(ref string, contents []byte) error {
	const op = "cloud/storage/drive.Put"
	// check if file already exists
	id, err := d.fileId(ref)
	if err != nil && !os.IsNotExist(err) {
		return errors.E(op, errors.IO, err)
	}
	if id != "" {
		// if it does, delete it to ensure uniqueness because Google Drive allows
		// multiple files with the same name to coexist in the same folder. See:
		// https://developers.google.com/drive/v3/reference/files#properties
		if err := d.Delete(ref); err != nil {
			return err
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
	if err != nil {
		if os.IsNotExist(err) {
			// nothing to delete
			return nil
		}
		return errors.E(op, errors.IO, err)
	}
	if err := d.files.Delete(id).Do(); err != nil {
		return errors.E(op, errors.IO, err)
	}
	d.ids.Remove(ref)
	return nil
}

// fileId returns the file ID of the first file found under the given name.
func (d *driveImpl) fileId(name string) (string, error) {
	// try cache first
	if id, ok := d.ids.Get(name); ok {
		return id.(string), nil
	}
	q := fmt.Sprintf("name='%s'", name)
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
