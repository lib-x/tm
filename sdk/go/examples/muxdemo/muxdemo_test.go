package muxdemo

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	tm "github.com/lib-x/tm/sdk/go"
	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
)

func TestRunCrossedPipes(t *testing.T) {
	// Real OS pipes have small buffers. Synchronous full-window writes in both
	// directions deadlock here even though the same code can pass over TCP.
	serverIn, clientOut, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	clientIn, serverOut, err := os.Pipe()
	if err != nil {
		serverIn.Close()
		clientOut.Close()
		t.Fatal(err)
	}
	for _, file := range []*os.File{serverIn, clientOut, clientIn, serverOut} {
		t.Cleanup(func() { file.Close() })
	}
	var report bytes.Buffer
	results := make(chan error, 2)
	go func() { results <- Run(serverIn, serverOut, false, io.Discard) }()
	go func() { results <- Run(clientIn, clientOut, true, &report) }()
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("crossed-pipe demo did not finish")
		}
	}
	for _, line := range []string{
		"PASS terminal 23 bytes\n",
		"PASS events 69 bytes\n",
		"PASS file 262144 bytes\n",
		"PASS clipboard 27 bytes\n",
		"PASS screenshot 68 bytes\n",
		"PASS all 5 streams\n",
	} {
		if !strings.Contains(report.String(), line) {
			t.Errorf("missing %q in %q", line, report.String())
		}
	}
}

// serverPair creates the same serial owner used by Run, exposing queued writes
// so tests can withhold peer credits and confirm when consumption occurs.
func serverPair(t *testing.T, peerSettings *pb.Settings) (*driver, *tm.Session) {
	t.Helper()
	peer, err := tm.NewSession(tm.Initiator, peerSettings, []string{profile})
	if err != nil {
		t.Fatal(err)
	}
	server, err := tm.NewSession(tm.Responder, tm.DefaultSettings(), []string{profile})
	if err != nil {
		t.Fatal(err)
	}
	hello, err := peer.Start()
	if err != nil {
		t.Fatal(err)
	}
	r, err := server.Receive(hello)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Receive(r.Outbound[0]); err != nil {
		t.Fatal(err)
	}
	return &driver{session: server, report: io.Discard, streams: make(map[uint32]*stream), usedRoutes: make(map[string]bool)}, peer
}

func receive(t *testing.T, d *driver, packet *pb.Packet) {
	t.Helper()
	r, err := d.session.Receive(packet)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Outbound) != 0 {
		t.Fatal("unexpected automatic packets in test")
	}
	if err := d.events(r.Events); err != nil {
		t.Fatal(err)
	}
	if err := d.pump(); err != nil {
		t.Fatal(err)
	}
}

func drain(t *testing.T, d *driver, peer *tm.Session) []tm.Event {
	t.Helper()
	var events []tm.Event
	for len(d.out) > 0 {
		job := d.out[0]
		d.out = d.out[1:]
		for _, packet := range job.packets {
			r, err := peer.Receive(packet)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Outbound) != 0 {
				t.Fatal("unexpected automatic peer response")
			}
			events = append(events, r.Events...)
		}
		if job.after != nil {
			if err := job.after(); err != nil {
				t.Fatal(err)
			}
		}
		if err := d.pump(); err != nil {
			t.Fatal(err)
		}
	}
	return events
}

func TestOpaqueEchoRetainsQueueUntilCreditReturns(t *testing.T) {
	settings := tm.DefaultSettings()
	settings.StreamWindow = 4096
	d, peer := serverPair(t, settings)
	id, open, err := peer.Open(&pb.Open{Profile: profile, Route: "file"})
	if err != nil {
		t.Fatal(err)
	}
	receive(t, d, open)
	drain(t, d, peer)
	payload := make([]byte, 8192)
	for i := range payload {
		payload[i] = byte(255 - i%256) // Deliberately differs from the file fixture.
	}
	packet, err := peer.Send(id, payload, true)
	if err != nil {
		t.Fatal(err)
	}
	receive(t, d, packet)
	st := d.streams[id]
	if !st.writing || len(st.queue) != 1 || len(st.queue[0].payload) != len(payload) {
		t.Fatal("echo bytes were released before the write completed")
	}
	var echo []byte
	var fin bool
	collect := func(events []tm.Event) {
		for _, event := range events {
			if event.Kind == tm.ReceivedData {
				echo = append(echo, event.Payload...)
				fin = event.Fin
			}
		}
	}
	collect(drain(t, d, peer))
	if len(echo) != 4096 || fin || st.done || len(st.queue) != 1 || len(st.queue[0].payload) != 4096 {
		t.Fatalf("credit stall lost queued echo: echo=%d fin=%v done=%v queue=%v", len(echo), fin, st.done, st.queue)
	}
	windows, err := peer.Consume(id, uint32(len(echo)))
	if err != nil {
		t.Fatal(err)
	}
	for _, window := range windows {
		// Receive(WINDOW) has zero events. Pumping must still retry the queue.
		receive(t, d, window)
		collect(drain(t, d, peer))
	}
	if !bytes.Equal(echo, payload) || !fin || !st.done || len(st.queue) != 0 {
		t.Fatalf("opaque echo did not finish: bytes=%d fin=%v done=%v", len(echo), fin, st.done)
	}
	if err := d.events([]tm.Event{{Kind: tm.ReceivedGoAway}}); err == nil {
		t.Fatal("server accepted GOAWAY after only one route")
	}
}

func TestServerRefusesDuplicateAndUnknownRoutes(t *testing.T) {
	d, peer := serverPair(t, tm.DefaultSettings())
	for _, route := range append(append([]string{}, routes...), "terminal", "unknown") {
		id, open, err := peer.Open(&pb.Open{Profile: profile, Route: route})
		if err != nil {
			t.Fatal(err)
		}
		receive(t, d, open)
		events := drain(t, d, peer)
		if id <= 9 {
			if len(events) != 1 || events[0].Kind != tm.Accepted {
				t.Fatalf("route %s was not accepted: %v", route, events)
			}
		} else if len(events) != 1 || events[0].Kind != tm.StreamReset || events[0].Code != pb.ErrorCode_REFUSED {
			t.Fatalf("route %s was not refused: %v", route, events)
		}
	}
	if len(d.order) != 5 || len(d.usedRoutes) != 5 {
		t.Fatal("server accepted more than five unique routes")
	}
	if err := d.events([]tm.Event{{Kind: tm.ReceivedGoAway}}); err == nil {
		t.Fatal("server accepted GOAWAY with unfinished routes")
	}
}

func TestRunRejectsEarlyEOF(t *testing.T) {
	if err := Run(bytes.NewReader(nil), io.Discard, false, io.Discard); !errors.Is(err, io.EOF) {
		t.Fatalf("early EOF error = %v", err)
	}
}

func TestClientRejectsBadEchoAndPrematureFIN(t *testing.T) {
	for _, event := range []tm.Event{
		{Kind: tm.ReceivedData, ID: 1, Payload: []byte("wrong")},
		{Kind: tm.ReceivedData, ID: 1, Fin: true},
	} {
		d := &driver{client: true, streams: map[uint32]*stream{1: {id: 1, route: "terminal", payload: []byte("hello"), sent: 5, sentFin: true}}}
		if err := d.events([]tm.Event{event}); err == nil {
			t.Fatalf("invalid echo was accepted: %v", event)
		}
	}
}

func TestRunWritesAutomaticPacketsBeforeRefusal(t *testing.T) {
	peer, err := tm.NewSession(tm.Initiator, tm.DefaultSettings(), []string{profile})
	if err != nil {
		t.Fatal(err)
	}
	hello, err := peer.Start()
	if err != nil {
		t.Fatal(err)
	}
	var input, output bytes.Buffer
	for _, packet := range []*pb.Packet{
		hello,
		{Body: &pb.Packet_Ping{Ping: &pb.Ping{Token: 42}}},
		{StreamId: 1, Body: &pb.Packet_Open{Open: &pb.Open{Profile: "forbidden.v1", Route: "terminal"}}},
		{Body: &pb.Packet_GoAway{GoAway: &pb.GoAway{Code: pb.ErrorCode_CANCELLED}}},
	} {
		if err := tm.WritePacket(&input, packet); err != nil {
			t.Fatal(err)
		}
	}
	if err := Run(&input, &output, false, io.Discard); err == nil || !strings.Contains(err.Error(), "before all 5") {
		t.Fatalf("premature GOAWAY error = %v", err)
	}
	welcome, err := tm.ReadPacket(&output)
	if err != nil || welcome.GetWelcome() == nil {
		t.Fatalf("welcome = %v, %v", welcome, err)
	}
	pong, err := tm.ReadPacket(&output)
	if err != nil || pong.GetPong().GetToken() != 42 {
		t.Fatalf("pong = %v, %v", pong, err)
	}
	reset, err := tm.ReadPacket(&output)
	if err != nil || reset.GetReset_() == nil || reset.GetReset_().Code != pb.ErrorCode_REFUSED {
		t.Fatalf("profile refusal = %v, %v", reset, err)
	}
	if output.Len() != 0 {
		t.Fatal("unexpected packets after refusal")
	}
}

func TestServerEchoesEmptyFIN(t *testing.T) {
	d, peer := serverPair(t, tm.DefaultSettings())
	id, open, err := peer.Open(&pb.Open{Profile: profile, Route: "terminal"})
	if err != nil {
		t.Fatal(err)
	}
	receive(t, d, open)
	drain(t, d, peer)
	packet, err := peer.Send(id, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	receive(t, d, packet)
	events := drain(t, d, peer)
	if len(events) != 1 || events[0].Kind != tm.ReceivedData || !events[0].Fin || len(events[0].Payload) != 0 || !d.streams[id].done {
		t.Fatalf("empty FIN echo = %v", events)
	}
}

// failingGoAwayWriter changes only the final shutdown packet on the wire. Both
// endpoints still complete all real streams through Run before this failure.
type failingGoAwayWriter struct {
	io.Writer
	changed bool
}

func (w *failingGoAwayWriter) Write(frame []byte) (int, error) {
	packet, err := tm.Decode(frame)
	if err != nil {
		return 0, err
	}
	if goaway := packet.GetGoAway(); goaway != nil {
		goaway.Code = pb.ErrorCode_INTERNAL
		goaway.Reason = "test peer failure"
		w.changed = true
		if err := tm.WritePacket(w.Writer, packet); err != nil {
			return 0, err
		}
		return len(frame), nil
	}
	return w.Writer.Write(frame)
}

func TestRunRejectsErrorGoAwayAfterAllStreamsComplete(t *testing.T) {
	for _, targetInitiator := range []bool{true, false} {
		name := "responder"
		if targetInitiator {
			name = "initiator"
		}
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			t.Cleanup(func() { client.Close(); server.Close() })
			for _, conn := range []net.Conn{client, server} {
				if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			clientWriter, serverWriter := io.Writer(client), io.Writer(server)
			injected := &failingGoAwayWriter{}
			if targetInitiator {
				injected.Writer = server
				serverWriter = injected
			} else {
				injected.Writer = client
				clientWriter = injected
			}
			type result struct {
				initiator bool
				err       error
				report    string
			}
			results := make(chan result, 2)
			for _, endpoint := range []struct {
				initiator bool
				conn      net.Conn
				writer    io.Writer
			}{{true, client, clientWriter}, {false, server, serverWriter}} {
				go func() {
					var report bytes.Buffer
					err := Run(endpoint.conn, endpoint.writer, endpoint.initiator, &report)
					endpoint.conn.Close()
					results <- result{endpoint.initiator, err, report.String()}
				}()
			}
			var target, initiator result
			for range 2 {
				r := <-results
				if r.initiator == targetInitiator {
					target = r
				}
				if r.initiator {
					initiator = r
				}
			}
			if !injected.changed {
				t.Fatal("peer never sent the error GOAWAY after completing the streams")
			}
			if target.err == nil || !strings.Contains(target.err.Error(), "GOAWAY") || !strings.Contains(target.err.Error(), "INTERNAL") {
				t.Fatalf("error GOAWAY must fail Run, got %v", target.err)
			}
			if strings.Contains(target.report, "PASS all 5 streams") {
				t.Fatalf("error GOAWAY produced a success report: %s", target.report)
			}
			if strings.Count(initiator.report, "PASS ") != len(routes) {
				t.Fatalf("regression must reach all five completed streams: %s", initiator.report)
			}
		})
	}
}
