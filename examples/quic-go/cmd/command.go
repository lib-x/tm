package cmd

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// Execute runs the CLI and reports failures on stderr.
func Execute() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "quic-demo:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	const usage = "usage: quic-demo {server [address] --cert cert.pem --key key.pem | client [address] --ca cert.pem | cert --cert cert.pem --key key.pem}"
	if len(args) == 0 {
		return errors.New(usage)
	}
	mode, args := args[0], args[1:]
	if mode != "server" && mode != "client" && mode != "cert" {
		return errors.New(usage)
	}
	address := "127.0.0.1:7001"
	if mode != "cert" && len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		address, args = args[0], args[1:]
	}
	flags := flag.NewFlagSet(mode, flag.ContinueOnError)
	flags.SetOutput(stderr)
	var cert, key, ca string
	if mode == "client" {
		flags.StringVar(&ca, "ca", "", "trusted server certificate PEM file (required)")
	} else {
		flags.StringVar(&cert, "cert", "", "server certificate PEM file (required)")
		flags.StringVar(&key, "key", "", "server private key PEM file (required)")
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New(usage)
	}
	if mode == "client" {
		if ca == "" {
			return errors.New("client requires --ca")
		}
		return client(address, ca, stdout)
	}
	if cert == "" || key == "" {
		return errors.New("--cert and --key are required")
	}
	if mode == "cert" {
		return generateCertificate(cert, key)
	}
	return server(address, cert, key, stdout)
}
