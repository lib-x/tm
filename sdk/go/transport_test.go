package tm

import (
	"bytes"
	"errors"
	"io"
	"testing"

	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
)

type shortWriter struct{ bytes.Buffer }

func (w *shortWriter) Write(b []byte) (int, error) { return w.Buffer.Write(b[:min(2, len(b))]) }

type brokenWriter struct {
	n   int
	err error
}

func (w brokenWriter) Write([]byte) (int, error) { return w.n, w.err }

func TestReadWritePacket(t *testing.T) {
	p := dataPacket(1, "test", true)
	var w shortWriter
	if err := WritePacket(&w, p); err != nil {
		t.Fatal(err)
	}
	got, err := ReadPacket(&w)
	if err != nil || string(got.GetData().Payload) != "test" {
		t.Fatal(got, err)
	}
	if _, err := ReadPacket(&w); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if err := WritePacket(brokenWriter{}, p); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	boom := errors.New("write failed")
	if err := WritePacket(brokenWriter{1, boom}, p); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	b := mustEncode(t, p)
	for i := 1; i < len(b); i++ {
		if _, err := ReadPacket(bytes.NewReader(b[:i])); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal(i, err)
		}
	}
}

func TestTransportResponseAndTerminalErrors(t *testing.T) {
	a, b := pair(t, DefaultSettings())
	id := openStream(t, a, b, true)
	ping, err := a.Ping(123)
	if err != nil {
		t.Fatal(err)
	}
	var writer bytes.Buffer
	transport := Transport{Session: b, Reader: bytes.NewReader(mustEncode(t, ping)), Writer: &writer}
	r, err := transport.Receive()
	if err != nil || len(r.Outbound) != 1 {
		t.Fatal(r, err)
	}
	p, err := ReadPacket(&writer)
	if err != nil || p.GetPong().Token != 123 {
		t.Fatal(p, err)
	}
	r, err = transport.Receive()
	if !errors.Is(err, io.EOF) || !b.Failed() || len(r.Events) != 1 || r.Events[0].ID != id {
		t.Fatal(r, err)
	}
	if _, err := b.Send(id, []byte("x"), false); !errors.Is(err, ErrFailed) {
		t.Fatal(err)
	}

	for _, bad := range [][]byte{{0, 2, 0, 1}, mustEncode(t, window(0, 1))} {
		a, b = pair(t, DefaultSettings())
		openStream(t, a, b, true)
		transport = Transport{Session: b, Reader: bytes.NewReader(bad), Writer: &writer}
		r, err = transport.Receive()
		if err == nil || !b.Failed() || len(r.Events) != 1 {
			t.Fatal(r, err)
		}
	}
	a, b = pair(t, DefaultSettings())
	openStream(t, a, b, true)
	transport = Transport{Session: b, Reader: bytes.NewReader(mustEncode(t, ping)), Writer: brokenWriter{}}
	r, err = transport.Receive()
	if !errors.Is(err, io.ErrShortWrite) || !b.Failed() || len(r.Events) != 1 {
		t.Fatal(r, err)
	}
}

func TestTransportWriteFailureLeavesCloseNotifications(t *testing.T) {
	a, b := pair(t, DefaultSettings())
	openStream(t, a, b, true)
	tx := Transport{Session: a, Writer: brokenWriter{}}
	p, err := a.Ping(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Write(p); !errors.Is(err, io.ErrShortWrite) || !a.Failed() {
		t.Fatal(err)
	}
	if events := a.Close(); len(events) != 1 || events[0].Code != pb.ErrorCode_INTERNAL {
		t.Fatal(events)
	}
}

func TestTransportInvalidBatchDoesNotWriteOrFail(t *testing.T) {
	a, _ := pair(t, DefaultSettings())
	var output bytes.Buffer
	tx := Transport{Session: a, Writer: &output}
	p, err := a.Ping(42)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Write(p, nil); !errors.Is(err, ErrArgument) || a.Failed() || output.Len() != 0 {
		t.Fatal(err)
	}
}
