// mux-demo exchanges five application byte streams over TCP or crossed stdio.
// It only echoes application payloads: it never executes a shell or changes the
// clipboard. Stdio has no built-in deadline; use a host runner timeout (15s is
// sufficient for the fixtures), for example: timeout 15s mux-demo stdio-client.
package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/lib-x/tm/sdk/go/examples/muxdemo"
)

const timeout = 15 * time.Second

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "mux-demo:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	const usage = "usage: mux-demo {server|client} [127.0.0.1:7000] | {stdio-server|stdio-client}"
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "stdio-server", "stdio-client":
		if len(args) != 1 {
			return errors.New(usage)
		}
		return muxdemo.Run(os.Stdin, os.Stdout, args[0] == "stdio-client", os.Stderr)
	case "server", "client":
		if len(args) > 2 {
			return errors.New(usage)
		}
	default:
		return errors.New(usage)
	}
	address := "127.0.0.1:7000"
	if len(args) == 2 {
		address = args[1]
	}
	var conn net.Conn
	var err error
	if args[0] == "server" {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}
		defer listener.Close()
		if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(os.Stdout, "LISTEN", listener.Addr()); err != nil {
			return err
		}
		conn, err = listener.Accept()
	} else {
		conn, err = net.DialTimeout("tcp", address, timeout)
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.(*net.TCPConn).SetNoDelay(true); err != nil {
		return err
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	return muxdemo.Run(conn, conn, args[0] == "client", os.Stdout)
}
