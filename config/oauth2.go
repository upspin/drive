// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The config package holds OAuth2 configuration data shared by the drive storage
// and the setupstorage-drive command.
package config // import "drive.upspin.io/config"

import (
	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
)

// OAuth2 holds OAuth configuration used by the Upspin Google Drive package. It is used by both
// the storage and the setup process.
var OAuth2 = &oauth2.Config{
	ClientID:     "756365541666-dbbsja2vlrl38j0r85f32cgl3sj6n8k9.apps.googleusercontent.com",
	ClientSecret: "RfAusHn6sSN7YO2pErac0ggs",
	Endpoint: oauth2.Endpoint{
		AuthURL:  "https://accounts.google.com/o/oauth2/auth",
		TokenURL: "https://accounts.google.com/o/oauth2/token",
	},
	RedirectURL: "urn:ietf:wg:oauth:2.0:oob",
	Scopes:      []string{drive.DriveAppdataScope},
}
