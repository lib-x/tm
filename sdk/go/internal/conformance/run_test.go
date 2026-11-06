package conformance

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	tm "github.com/lib-x/tm/sdk/go"
	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestEmitVerifySemanticTranscript(t *testing.T) {
	var out bytes.Buffer
	if err := Run([]string{"emit"}, nil, &out); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(out.String(), "\n"); got != 10 {
		t.Fatal(got)
	}
	if err := Run([]string{"verify"}, bytes.NewReader(out.Bytes()), nil); err != nil {
		t.Fatal(err)
	}
	// Reorder top-level fields and add an unknown nested field: compare models,
	// never re-encoded byte ordering or unknown fields.
	var reordered bytes.Buffer
	for _, p := range transcript() {
		if h := p.GetHello(); h != nil {
			h.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1})
		}
		b, err := tm.Encode(p)
		if err != nil {
			t.Fatal(err)
		}
		body := b[4:]
		if p.StreamId != 0 {
			_, n := protowire.ConsumeVarint(body[1:])
			sid := bytes.Clone(body[:n+1])
			body = append(bytes.Clone(body[n+1:]), sid...)
			b = append([]byte{0, 0, 0, byte(len(body))}, body...)
		}
		fmt.Fprintln(&reordered, hex.EncodeToString(b))
	}
	if err := Run([]string{"verify"}, &reordered, nil); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	for name, input := range map[string]string{
		"missing":     strings.Join(lines[:9], "\n"),
		"extra":       out.String() + lines[0] + "\n",
		"wrong order": strings.Join(append([]string{lines[1], lines[0]}, lines[2:]...), "\n"),
		"not hex":     "not hex\n",
		"empty":       "",
	} {
		t.Run(name, func(t *testing.T) {
			if err := Run([]string{"verify"}, strings.NewReader(input), nil); err == nil {
				t.Fatal("accepted malformed transcript")
			}
		})
	}
	changed := transcript()
	changed[4].GetData().Fin = false
	var invalid bytes.Buffer
	for _, p := range changed {
		b, err := tm.Encode(p)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(&invalid, hex.EncodeToString(b))
	}
	if err := verify(&invalid); err == nil {
		t.Fatal("accepted incorrect DATA semantics")
	}
}

// The helper executes the actual endpoint event loop in another process, with
// no test-runner text reaching stdout. Parent uses OS pipes as a duplex carrier.
func TestEndpointHelper(t *testing.T) {
	role := os.Getenv("TM_TEST_ENDPOINT_ROLE")
	if role == "" {
		return
	}
	if err := Run([]string{"endpoint", role}, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func TestEndpointProcesses(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	abR, abW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	baR, baW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []*os.File{abR, abW, baR, baW} {
		t.Cleanup(func() { _ = f.Close() })
	}
	a := exec.CommandContext(ctx, exe, "-test.run=^TestEndpointHelper$")
	b := exec.CommandContext(ctx, exe, "-test.run=^TestEndpointHelper$")
	a.Env = append(os.Environ(), "TM_TEST_ENDPOINT_ROLE=initiator")
	b.Env = append(os.Environ(), "TM_TEST_ENDPOINT_ROLE=responder")
	a.Stdin = baR
	a.Stdout = abW
	b.Stdin = abR
	b.Stdout = baW
	var ae, be bytes.Buffer
	a.Stderr = &ae
	b.Stderr = &be
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if a.Process != nil {
			_ = a.Process.Kill()
		}
	})
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if b.Process != nil {
			_ = b.Process.Kill()
		}
	})
	for _, f := range []*os.File{abR, abW, baR, baW} {
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Wait(); err != nil {
		t.Fatalf("initiator: %v: %s", err, &ae)
	}
	if err := b.Wait(); err != nil {
		t.Fatalf("responder: %v: %s", err, &be)
	}
	if ae.Len() != 0 || be.Len() != 0 {
		t.Fatal(ae.String(), be.String())
	}
}

func TestEndpointRejectsUnexpectedDataAndEarlyEOF(t *testing.T) {
	for _, role := range []string{"initiator", "responder"} {
		if err := Run([]string{"endpoint", role}, strings.NewReader(""), new(bytes.Buffer)); err == nil {
			t.Fatal("accepted early EOF")
		}
	}
	ps := transcript()[:2]
	ps = append(ps[1:], &pb.Packet{StreamId: 1, Body: &pb.Packet_OpenAck{OpenAck: &pb.OpenAck{}}}, &pb.Packet{StreamId: 1, Body: &pb.Packet_Data{Data: &pb.Data{Payload: []byte("incorrect"), Fin: true}}})
	var input bytes.Buffer
	for _, p := range ps {
		b, err := tm.Encode(p)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(&input, hex.EncodeToString(b))
	}
	if err := Run([]string{"endpoint", "initiator"}, &input, new(bytes.Buffer)); err == nil {
		t.Fatal("accepted incorrect response")
	}
}
