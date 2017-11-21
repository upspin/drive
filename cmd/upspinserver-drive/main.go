// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command upspinserver-drive is a combined DirServer and StoreServer for use on
// stand-alone machines. It provides the production implementations of the dir and
// store servers (dir/server and store/server) with support for storage in Google Drive.
package main // import "drive.upspin.io/cmd/upspinserver-drive"

import (
	"upspin.io/cloud/https"
	"upspin.io/serverutil/upspinserver"

	// Storage on Google Drive.
	_ "drive.upspin.io/cloud/storage/drive"
)

func main() {
	ready := upspinserver.Main()
	https.ListenAndServe(ready, https.OptionsFromFlags())
}
