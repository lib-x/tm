package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/lib-x/tm/sdk/go/examples/muxdemo"
	"github.com/quic-go/quic-go"
)

const (
	alpn    = "tmp-demo-v1"
	timeout = 15 * time.Second
)

func quicConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout:  timeout,
		MaxIdleTimeout:        timeout,
		MaxIncomingStreams:    1,
		MaxIncomingUniStreams: -1,
	}
}

func server(address, certPath, keyPath string, log io.Writer) error {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	listener, err := quic.ListenAddr(address, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{alpn},
	}, quicConfig())
	if err != nil {
		return err
	}
	defer listener.Close()
	if _, err := fmt.Fprintln(log, "LISTEN", listener.Addr()); err != nil {
		return err
	}
	conn, err := listener.Accept(ctx)
	if err != nil {
		return err
	}
	defer conn.CloseWithError(1, "demo interrupted")
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	if err := exchange(ctx, stream, false, log); err != nil {
		return err
	}
	// The client's FIN follows its receipt of our FIN, so our final GOAWAY
	// has reached the peer before we tear down the connection.
	return conn.CloseWithError(0, "demo complete")
}

func client(address, caPath string, log io.Writer) error {
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("--ca contains no certificates")
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := quic.DialAddr(ctx, address, &tls.Config{
		RootCAs:    roots,
		ServerName: host,
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{alpn},
	}, quicConfig())
	if err != nil {
		return err
	}
	defer conn.CloseWithError(1, "demo interrupted")
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if err := exchange(ctx, stream, true, log); err != nil {
		return err
	}
	// Keep the connection alive until the server sees our FIN. Closing it
	// here could discard buffered writes, including that FIN.
	select {
	case <-conn.Context().Done():
		if remote, ok := errors.AsType[*quic.ApplicationError](context.Cause(conn.Context())); ok && remote.Remote && remote.ErrorCode == 0 {
			return nil
		}
		return context.Cause(conn.Context())
	case <-ctx.Done():
		return ctx.Err()
	}
}

func exchange(ctx context.Context, stream *quic.Stream, initiator bool, log io.Writer) error {
	deadline, _ := ctx.Deadline()
	if err := stream.SetDeadline(deadline); err != nil {
		return err
	}
	if err := muxdemo.Run(stream, stream, initiator, log); err != nil {
		return err
	}
	// The responder sends FIN first. The initiator sends FIN only after
	// reading that EOF; EOF is ordered after every byte of reciprocal GOAWAY.
	if !initiator {
		if err := stream.Close(); err != nil {
			return err
		}
	}
	n, err := io.Copy(io.Discard, stream)
	if err != nil {
		return fmt.Errorf("await peer stream FIN: %w", err)
	}
	if n != 0 {
		return errors.New("unexpected bytes after reciprocal GOAWAY")
	}
	if initiator {
		return stream.Close()
	}
	return nil
}
