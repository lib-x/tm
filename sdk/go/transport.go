package tm

import (
	"io"

	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
)

// Transport binds a single-owner Session to ordered bytes. It does not own or
// close Reader/Writer and starts no background work. All methods, including
// direct Session operations, must be serialized by the caller.
type Transport struct {
	Session *Session
	Reader  io.Reader
	Writer  io.Writer
}

// Write sends operation results in order. Invalid packet arguments produce no
// writes or state changes. A write error fails the session; call Session.Close
// to retrieve stream failure events. Never retry a partial write.
func (t *Transport) Write(packets ...*pb.Packet) error {
	if t.Session.Failed() {
		return ErrFailed
	}
	for _, p := range packets {
		if err := validatePacket(p); err != nil {
			return ErrArgument
		}
	}
	for _, p := range packets {
		if err := WritePacket(t.Writer, p); err != nil {
			t.Session.state = stateFailed
			return err
		}
	}
	return nil
}

// Receive reads one packet, applies it, and sends automatic outbound before
// returning application events. EOF, malformed framing, and write errors fail
// the session and return Reset(INTERNAL) events for its active streams.
func (t *Transport) Receive() (Result, error) {
	if t.Session.Failed() {
		return Result{}, ErrFailed
	}
	p, err := ReadPacket(t.Reader)
	if err != nil {
		return Result{Events: t.Session.Close()}, err
	}
	r, err := t.Session.Receive(p)
	if err != nil {
		return Result{Events: t.Session.Close()}, err
	}
	if err := t.Write(r.Outbound...); err != nil {
		return Result{Events: t.Session.Close()}, err
	}
	return r, nil
}
