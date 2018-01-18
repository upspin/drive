// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The upspin-setupstorage-drive command is an external upspin subcommand that
// executes the second step in establishing an upspinserver for Google Drive.
// Run upspin setupstorage-drive -help for more information.
package main // import "drive.upspin.io/cmd/upspin-setupstorage-drive"

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"upspin.io/subcmd"

	"drive.upspin.io/config"
	"golang.org/x/oauth2"
)

const help = `
setupstorage-drive is the second step in establishing an upspinserver,
It sets up Google Drive storage for your Upspin installation. You may
skip this step if you wish to store Upspin data on your server's local
disk.

The first step is 'setupdomain' and the final step is 'setupserver'.
setupstorage-drive will add the Upspin Drive Storage application to your
Drive account. It then writes the obtained OAuth2 token information to
$where/$domain/serviceaccount.json and updates the server configuration
files in that directory to use the selected account.

Simply follow the on-screen instructions which will guide you through the
process.
`

type state struct{ *subcmd.State }

func main() {
	const name = "setupstorage-drive"

	log.SetFlags(0)
	log.SetPrefix("upspin setupstorage-drive: ")

	where := flag.String("where", filepath.Join(os.Getenv("HOME"), "upspin", "deploy"), "`directory` to store private configuration files")
	domain := flag.String("domain", "", "domain `name` for this Upspin installation")

	s := &state{State: subcmd.NewState(name)}
	s.ParseFlags(flag.CommandLine, os.Args[1:], help, "setupstorage-drive -domain=<name>")
	if *domain == "" {
		s.Exitf("%s\nthe -domain flag must be provided")
	}

	tok := s.tokenFromWeb()
	cfgPath := filepath.Join(*where, *domain)
	cfg := s.ReadServerConfig(cfgPath)
	cfg.StoreConfig = []string{
		"backend=Drive",
		"accessToken=" + tok.AccessToken,
		"tokenType=" + tok.TokenType,
		"refreshToken=" + tok.RefreshToken,
		"expiry=" + tok.Expiry.Format(time.RFC3339),
	}
	s.WriteServerConfig(cfgPath, cfg)

	fmt.Fprintf(os.Stderr, "You should now deploy the upspinserver binary and run 'upspin setupserver'.\n")
	s.ExitNow()
}

// tokenFromWeb attempts to obtain an OAuth2 token via the web and returns it.
func (s *state) tokenFromWeb() *oauth2.Token {
	authURL := config.OAuth2.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Open this URL in your browser to obtain an authorization code:\n\t%s\n", authURL)
	var code string
	fmt.Print("Auth code: ")
	if _, err := fmt.Scan(&code); err != nil {
		s.Exitf("unable to read authorization code %v", err)
	}
	tok, err := config.OAuth2.Exchange(oauth2.NoContext, code)
	if err != nil {
		s.Exitf("unable to retrieve token from web %v", err)
	}
	return tok
}
