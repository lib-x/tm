// Package muxdemo runs the five-route byte echo demonstration over ordered I/O.
// It only echoes application payloads: it never executes a shell or changes the
// clipboard. Callers own transport setup, shutdown, deadlines and diagnostics.
package muxdemo

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"slices"

	tm "github.com/lib-x/tm/sdk/go"
	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
)

const (
	profile   = "bytes.v1"
	chunkSize = 1024
)

var routes = []string{"terminal", "events", "file", "clipboard", "screenshot"}

func fixtures() ([][]byte, error) {
	file := make([]byte, 262144)
	for i := range file {
		file[i] = byte(i % 251)
	}
	image, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=")
	if err != nil {
		return nil, err
	}
	return [][]byte{
		[]byte("printf '你好 TMP\\n'\r\n"),
		[]byte("{\"type\":\"started\",\"seq\":1}\n{\"type\":\"output\",\"seq\":2,\"text\":\"你好\"}\n"),
		file,
		[]byte("TMP clipboard: 你好 🌍\n"),
		image,
	}, nil
}

type echoChunk struct {
	payload []byte
	fin     bool
}

type stream struct {
	id                               uint32
	route                            string
	payload                          []byte // Client fixture only; the server accepts arbitrary bytes.
	accepted, sentFin, done, writing bool
	sent, echoed                     int
	queue                            []echoChunk
}

type writeJob struct {
	packets []*pb.Packet
	after   func() error
}

type readResult struct {
	packet *pb.Packet
	err    error
}

type driver struct {
	session                       *tm.Session
	client                        bool
	report                        io.Writer
	streams                       map[uint32]*stream
	order                         []*stream
	usedRoutes                    map[string]bool
	out                           []writeJob
	barrier, stopping, sentGoAway bool
}

// Run exchanges five byte streams and reciprocal GOAWAY over an ordered carrier.
// With initiator true it sends and verifies deterministic fixtures; otherwise it
// echoes arbitrary bytes on five unique allowed routes. PASS lines go to log.
//
// Run owns every Session call. Two workers only perform byte I/O, allowing
// crossed pipes to drain while a full send window is being pumped. The caller
// owns the reader/writer and must close them to unblock I/O after an error.
// Set carrier deadlines or use a host runner timeout; Run has no built-in timer.
func Run(reader io.Reader, writer io.Writer, initiator bool, log io.Writer) error {
	client, report := initiator, log
	role := tm.Responder
	if client {
		role = tm.Initiator
	}
	session, err := tm.NewSession(role, tm.DefaultSettings(), []string{profile})
	if err != nil {
		return err
	}
	defer session.Close()
	d := &driver{session: session, client: client, report: report, streams: make(map[uint32]*stream), usedRoutes: make(map[string]bool)}
	if client {
		hello, err := session.Start()
		if err != nil {
			return err
		}
		d.enqueue([]*pb.Packet{hello}, nil)
	}
	stop := make(chan struct{})
	defer close(stop)
	readNext := make(chan struct{})
	reads := make(chan readResult)
	writes := make(chan writeJob)
	written := make(chan error)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-readNext:
			}
			packet, err := tm.ReadPacket(reader)
			select {
			case reads <- readResult{packet, err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	go func() {
		for {
			var job writeJob
			select {
			case <-stop:
				return
			case job = <-writes:
			}
			var err error
			for _, packet := range job.packets {
				if err = tm.WritePacket(writer, packet); err != nil {
					break
				}
			}
			select {
			case written <- err:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var reading, writing bool
	var active writeJob
	for {
		if d.stopping && !writing && len(d.out) == 0 {
			if client {
				_, err := fmt.Fprintln(report, "PASS all 5 streams")
				return err
			}
			return nil
		}
		var requestRead chan struct{}
		if !reading && !d.barrier && !d.stopping {
			requestRead = readNext
		}
		var requestWrite chan writeJob
		var next writeJob
		if !writing && len(d.out) > 0 {
			requestWrite, next = writes, d.out[0]
		}
		select {
		case requestRead <- struct{}{}:
			reading = true
		case requestWrite <- next:
			active, writing = next, true
			d.out[0] = writeJob{}
			d.out = d.out[1:]
		case result := <-reads:
			reading = false
			if result.err != nil {
				return fmt.Errorf("receive before reciprocal GOAWAY: %w", result.err)
			}
			r, err := session.Receive(result.packet)
			if err != nil {
				return err
			}
			if goaway := result.packet.GetGoAway(); goaway != nil && goaway.Code != pb.ErrorCode_CANCELLED {
				return fmt.Errorf("peer GOAWAY error %s: %s", goaway.Code, goaway.Reason)
			}
			// The SDK refuses unnegotiated profiles before emitting Opened.
			// Apply this demo's uniform REFUSED policy to that wire response.
			if !client && result.packet.GetOpen() != nil {
				for _, packet := range r.Outbound {
					if reset := packet.GetReset_(); reset != nil && reset.Code == pb.ErrorCode_UNSUPPORTED {
						reset.Code = pb.ErrorCode_REFUSED
					}
				}
			}
			if len(r.Outbound) > 0 || result.packet.GetGoAway() != nil {
				// Handle events only after all automatic packets are written.
				// GOAWAY also waits for earlier echo write confirmations before
				// checking whether the accepted streams are complete.
				d.barrier = true
				d.enqueue(r.Outbound, func() error {
					d.barrier = false
					return d.events(r.Events)
				})
			} else if err := d.events(r.Events); err != nil {
				return err
			}
			// WINDOW packets produce no events but can unblock any stream.
			if err := d.pump(); err != nil {
				return err
			}
		case err := <-written:
			writing = false
			if err != nil {
				return fmt.Errorf("write: %w", err)
			}
			if active.after != nil {
				if err := active.after(); err != nil {
					return err
				}
			}
			active = writeJob{}
			if err := d.pump(); err != nil {
				return err
			}
		}
	}
}

func (d *driver) enqueue(packets []*pb.Packet, after func() error) {
	d.out = append(d.out, writeJob{packets: packets, after: after})
}

func (d *driver) events(events []tm.Event) error {
	for _, event := range events {
		switch event.Kind {
		case tm.Ready:
			if !slices.Contains(d.session.Profiles(), profile) {
				return errors.New("bytes.v1 was not negotiated")
			}
			if d.client {
				payloads, err := fixtures()
				if err != nil {
					return err
				}
				for i, route := range routes {
					id, packet, err := d.session.Open(&pb.Open{Profile: profile, Route: route, Class: pb.Class_INTERACTIVE})
					if err != nil {
						return err
					}
					st := &stream{id: id, route: route, payload: payloads[i]}
					d.streams[id], d.order = st, append(d.order, st)
					d.enqueue([]*pb.Packet{packet}, nil)
				}
			}
		case tm.Opened:
			if d.client {
				return errors.New("unexpected server-initiated stream")
			}
			route := event.Open.Route
			if event.Open.Profile != profile || !slices.Contains(routes, route) || d.usedRoutes[route] {
				packets, err := d.session.Reset(event.ID, pb.ErrorCode_REFUSED, "route/profile refused")
				if err != nil {
					return err
				}
				d.enqueue(packets, nil)
				continue
			}
			packet, err := d.session.Accept(event.ID)
			if err != nil {
				return err
			}
			st := &stream{id: event.ID, route: route, accepted: true}
			d.streams[st.id], d.order = st, append(d.order, st)
			d.usedRoutes[route] = true
			d.enqueue([]*pb.Packet{packet}, nil)
		case tm.Accepted:
			st := d.streams[event.ID]
			if !d.client || st == nil {
				return errors.New("unexpected stream acceptance")
			}
			st.accepted = true
		case tm.ReceivedData:
			st := d.streams[event.ID]
			if st == nil || st.done {
				return fmt.Errorf("unexpected data on stream %d", event.ID)
			}
			if !d.client {
				st.queue = append(st.queue, echoChunk{event.Payload, event.Fin})
				continue
			}
			n := len(event.Payload)
			if n > len(st.payload)-st.echoed || n > st.sent-st.echoed || !bytes.Equal(event.Payload, st.payload[st.echoed:st.echoed+n]) {
				return fmt.Errorf("%s: echo differs at byte %d", st.route, st.echoed)
			}
			st.echoed += n
			if event.Fin && (!st.sentFin || st.echoed != len(st.payload)) {
				return fmt.Errorf("%s: premature FIN after %d bytes", st.route, st.echoed)
			}
			if err := d.consume(st.id, n); err != nil {
				return err
			}
			if event.Fin {
				st.done = true
				if _, err := fmt.Fprintf(d.report, "PASS %s %d bytes\n", st.route, st.echoed); err != nil {
					return err
				}
			}
		case tm.StreamReset:
			return fmt.Errorf("stream %d reset: %s (%s)", event.ID, event.Code, event.Reason)
		case tm.ReceivedGoAway:
			if !d.complete() || (d.client && !d.sentGoAway) {
				return errors.New("GOAWAY before all 5 streams completed")
			}
			if !d.client {
				if err := d.goAway(); err != nil {
					return err
				}
			}
			d.stopping = true
		}
	}
	return nil
}

func (d *driver) consume(id uint32, n int) error {
	if n == 0 {
		return nil // Sending/receiving an empty FIN can already reap the stream.
	}
	packets, err := d.session.Consume(id, uint32(n))
	if err != nil {
		return err
	}
	d.enqueue(packets, nil)
	return nil
}

func (d *driver) complete() bool {
	if len(d.order) != len(routes) {
		return false
	}
	for _, st := range d.order {
		if !st.done {
			return false
		}
	}
	return true
}

func (d *driver) goAway() error {
	packets, err := d.session.GoAway(pb.ErrorCode_CANCELLED, "all 5 streams complete")
	if err != nil {
		return err
	}
	d.sentGoAway = true
	d.enqueue(packets, nil)
	return nil
}

func (d *driver) pump() error {
	if d.barrier || d.stopping {
		return nil
	}
	for {
		progress := false
		for _, st := range d.order {
			if !st.accepted || st.done || st.writing {
				continue
			}
			if d.client {
				if st.sentFin {
					continue
				}
				n := min(chunkSize, len(st.payload)-st.sent)
				fin := st.sent+n == len(st.payload)
				packet, err := d.session.Send(st.id, st.payload[st.sent:st.sent+n], fin)
				if errors.Is(err, tm.ErrWouldBlock) {
					continue
				}
				if err != nil {
					return err
				}
				st.sent, st.sentFin = st.sent+n, fin
				d.enqueue([]*pb.Packet{packet}, nil)
			} else {
				if len(st.queue) == 0 {
					continue
				}
				chunk := st.queue[0]
				n := min(chunkSize, len(chunk.payload))
				fin := chunk.fin && n == len(chunk.payload)
				packet, err := d.session.Send(st.id, chunk.payload[:n], fin)
				if errors.Is(err, tm.ErrWouldBlock) {
					continue
				}
				if err != nil {
					return err
				}
				st.writing = true
				d.enqueue([]*pb.Packet{packet}, func() error {
					// Retain data and receive credit until the echo is on the wire.
					if err := d.consume(st.id, n); err != nil {
						return err
					}
					st.queue[0].payload = st.queue[0].payload[n:]
					if len(st.queue[0].payload) == 0 {
						st.queue[0] = echoChunk{}
						st.queue = st.queue[1:]
					}
					st.writing, st.done = false, fin
					return nil
				})
			}
			progress = true
		}
		if !progress {
			break
		}
	}
	if d.client && d.complete() && !d.sentGoAway {
		return d.goAway()
	}
	return nil
}
