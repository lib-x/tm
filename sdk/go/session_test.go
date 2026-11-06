package tm

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
	"google.golang.org/protobuf/proto"
)

func mustSession(t *testing.T, role Role, settings *pb.Settings) *Session {
	t.Helper()
	s, err := NewSession(role, settings, []string{"bytes.v1"})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func deliver(t *testing.T, s *Session, p *pb.Packet) Result {
	t.Helper()
	r, err := s.Receive(p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func pair(t *testing.T, settings *pb.Settings) (*Session, *Session) {
	t.Helper()
	a, b := mustSession(t, Initiator, settings), mustSession(t, Responder, settings)
	p, err := a.Start()
	if err != nil {
		t.Fatal(err)
	}
	r := deliver(t, b, p)
	if len(r.Outbound) != 1 || r.Events[0].Kind != Ready {
		t.Fatal(r)
	}
	r = deliver(t, a, r.Outbound[0])
	if r.Events[0].Kind != Ready {
		t.Fatal(r)
	}
	return a, b
}
func openStream(t *testing.T, a, b *Session, accept bool) uint32 {
	t.Helper()
	id, p, err := a.Open(&pb.Open{Profile: "bytes.v1", Route: "echo"})
	if err != nil {
		t.Fatal(err)
	}
	r := deliver(t, b, p)
	if len(r.Events) != 1 || r.Events[0].Kind != Opened {
		t.Fatal(r)
	}
	if accept {
		p, err = b.Accept(id)
		if err != nil {
			t.Fatal(err)
		}
		r = deliver(t, a, p)
		if r.Events[0].Kind != Accepted {
			t.Fatal(r)
		}
	}
	return id
}
func sendData(t *testing.T, a, b *Session, id uint32, data string, fin bool) Result {
	t.Helper()
	p, err := a.Send(id, []byte(data), fin)
	if err != nil {
		t.Fatal(err)
	}
	return deliver(t, b, p)
}
func consume(t *testing.T, a, b *Session, id, n uint32) {
	t.Helper()
	ps, err := a.Consume(id, n)
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 2 || ps[0].StreamId != id || ps[1].StreamId != 0 {
		t.Fatal(ps)
	}
	for _, p := range ps {
		if p.GetWindow().Credit != n {
			t.Fatal(p)
		}
		deliver(t, b, p)
	}
}
func dataPacket(id uint32, data string, fin bool) *pb.Packet {
	return &pb.Packet{StreamId: id, Body: &pb.Packet_Data{Data: &pb.Data{Payload: []byte(data), Fin: fin}}}
}
func goAwayPacket(last uint32) *pb.Packet {
	return &pb.Packet{Body: &pb.Packet_GoAway{GoAway: &pb.GoAway{LastStreamId: last}}}
}

func TestHandshakeValidationAndOwnership(t *testing.T) {
	settings := DefaultSettings()
	s := mustSession(t, Initiator, settings)
	settings.MaxData = 0
	p, err := s.Start()
	if err != nil {
		t.Fatal(err)
	}
	if p.GetHello().Settings.MaxData != 16384 {
		t.Fatal("settings aliased")
	}
	p.GetHello().Settings.MaxData = 1
	p.GetHello().Profiles[0] = "modified"
	if s.local.MaxData != 16384 || s.profiles[0] != "bytes.v1" {
		t.Fatal("hello aliased")
	}
	for _, settings := range []*pb.Settings{nil, {}, {MaxData: 1023, StreamWindow: 4096, ConnectionWindow: 65536, MaxStreams: 1}} {
		if _, err := NewSession(Initiator, settings, nil); !errors.Is(err, ErrArgument) {
			t.Fatal(err)
		}
	}
	for _, profiles := range [][]string{{"bytes.v1", "bytes.v1"}, {"Bad"}, {""}, {strings.Repeat("a", 65)}, make([]string, 33)} {
		if _, err := NewSession(Initiator, DefaultSettings(), profiles); !errors.Is(err, ErrArgument) {
			t.Fatal(profiles, err)
		}
	}
	a, err := NewSession(Initiator, DefaultSettings(), []string{"bytes.v1", "terminal.io.v1"})
	if err != nil {
		t.Fatal(err)
	}
	b := mustSession(t, Responder, DefaultSettings())
	p, err = a.Start()
	if err != nil {
		t.Fatal(err)
	}
	r := deliver(t, b, p)
	deliver(t, a, r.Outbound[0])
	profiles := a.Profiles()
	profiles[0] = "bad"
	if !reflect.DeepEqual(a.Profiles(), []string{"bytes.v1"}) {
		t.Fatal(a.Profiles())
	}
}

func TestHalfCloseOutstandingAndCredits(t *testing.T) {
	settings := DefaultSettings()
	settings.MaxStreams = 1
	a, b := pair(t, settings)
	id := openStream(t, a, b, true)
	r := sendData(t, a, b, id, "你好 TMP", true)
	if string(r.Events[0].Payload) != "你好 TMP" || !r.Events[0].Fin {
		t.Fatal(r)
	}
	if _, err := a.Send(id, []byte("late"), false); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	sendData(t, b, a, id, "ok", true)
	if a.streams[id] == nil || b.streams[id] == nil || a.connOutstanding != 2 || b.connOutstanding != 10 {
		t.Fatal("unread closed stream reclaimed")
	}
	if _, _, err := a.Open(&pb.Open{Profile: "bytes.v1"}); !errors.Is(err, ErrWouldBlock) {
		t.Fatal(err)
	}
	consume(t, b, a, id, 10)
	consume(t, a, b, id, 2)
	if len(a.streams) != 0 || len(b.streams) != 0 || a.connSendCredit != settings.ConnectionWindow || b.connSendCredit != settings.ConnectionWindow {
		t.Fatal("credit leak")
	}
	if got := openStream(t, a, b, true); got != 3 {
		t.Fatal(got)
	}
}

func TestFlowControlAndParameterAtomicity(t *testing.T) {
	settings := DefaultSettings()
	settings.StreamWindow = 4096
	settings.MaxData = 4096
	a, b := pair(t, settings)
	id := openStream(t, a, b, true)
	for _, o := range []*pb.Open{nil, {Profile: "other"}, {Profile: "bytes.v1", Class: 99}, {Profile: "bytes.v1", Route: strings.Repeat("x", 257)}, {Profile: "bytes.v1", Metadata: make([]byte, 4097)}, {Profile: "bytes.v1", Route: string([]byte{255})}} {
		before := a.highestLocal
		if _, _, err := a.Open(o); !errors.Is(err, ErrArgument) {
			t.Fatal(err)
		}
		if a.highestLocal != before {
			t.Fatal("open mutated")
		}
	}
	credit := a.connSendCredit
	for _, payload := range [][]byte{nil, make([]byte, 4097)} {
		if _, err := a.Send(id, payload, false); !errors.Is(err, ErrArgument) {
			t.Fatal(err)
		}
		if a.connSendCredit != credit || a.streams[id].localFin {
			t.Fatal("send mutated")
		}
	}
	sendData(t, a, b, id, strings.Repeat("x", 4096), false)
	credit = a.connSendCredit
	if _, err := a.Send(id, []byte("x"), true); !errors.Is(err, ErrWouldBlock) {
		t.Fatal(err)
	}
	if a.streams[id].localFin || a.connSendCredit != credit {
		t.Fatal("blocked send mutated")
	}
	if _, err := b.Consume(id, 4097); !errors.Is(err, ErrArgument) {
		t.Fatal(err)
	}
	if b.connOutstanding != 4096 {
		t.Fatal("consume mutated")
	}
	if ps, err := b.Consume(id, 0); err != nil || len(ps) != 0 {
		t.Fatal(ps, err)
	}
	if _, err := b.Reset(id, 99, ""); !errors.Is(err, ErrArgument) {
		t.Fatal(err)
	}
	if _, err := b.Reset(id, pb.ErrorCode_CANCELLED, strings.Repeat("x", MaxPacket)); !errors.Is(err, ErrArgument) {
		t.Fatal(err)
	}
	if _, err := b.GoAway(pb.ErrorCode_CANCELLED, string([]byte{255})); !errors.Is(err, ErrArgument) {
		t.Fatal(err)
	}
	if b.draining || b.streams[id] == nil {
		t.Fatal("invalid reset/goaway mutated")
	}
	consume(t, b, a, id, 4096)
	sendData(t, a, b, id, "x", false)
}

func TestConnectionCreditSharedAndIndependentDirections(t *testing.T) {
	settings := DefaultSettings()
	settings.MaxData = 65536
	settings.StreamWindow = 65536
	settings.ConnectionWindow = 65536
	a, b := pair(t, settings)
	id := openStream(t, a, b, true)
	second := openStream(t, a, b, true)
	sendData(t, a, b, id, strings.Repeat("x", 65536), false)
	if _, err := a.Send(second, []byte("x"), false); !errors.Is(err, ErrWouldBlock) {
		t.Fatal(err)
	}
	sendData(t, b, a, second, "independent", false)
	consume(t, b, a, id, 1)
	sendData(t, a, b, second, "x", false)
}

func TestResetRefundAndHistoricalPackets(t *testing.T) {
	a, b := pair(t, DefaultSettings())
	id := openStream(t, a, b, true)
	sendData(t, a, b, id, "abc", false)
	sendData(t, b, a, id, "xy", false)
	ps, err := b.Reset(id, pb.ErrorCode_CANCELLED, "stop")
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 2 || ps[0].GetReset_() == nil || ps[1].StreamId != 0 || ps[1].GetWindow().Credit != 3 {
		t.Fatal(ps)
	}
	r := deliver(t, a, ps[0])
	if r.Events[0].Kind != StreamReset || len(r.Outbound) != 1 || r.Outbound[0].GetWindow().Credit != 2 {
		t.Fatal(r)
	}
	deliver(t, a, ps[1])
	deliver(t, b, r.Outbound[0])
	if a.connSendCredit != a.peer.ConnectionWindow || b.connSendCredit != b.peer.ConnectionWindow || a.connOutstanding != 0 || b.connOutstanding != 0 {
		t.Fatal("refund leak")
	}
	for _, p := range []*pb.Packet{ps[0], window(id, 1), {StreamId: id, Body: &pb.Packet_OpenAck{OpenAck: &pb.OpenAck{}}}} {
		r := deliver(t, a, p)
		if len(r.Outbound)+len(r.Events) != 0 {
			t.Fatal(r)
		}
	}
	r = deliver(t, a, dataPacket(id, "late", false))
	if len(r.Events) != 0 || len(r.Outbound) != 1 || r.Outbound[0].StreamId != 0 || r.Outbound[0].GetWindow().Credit != 4 {
		t.Fatal(r)
	}
}

func TestPendingDataFailsBothDirections(t *testing.T) {
	for _, local := range []bool{true, false} {
		t.Run(map[bool]string{true: "local", false: "remote"}[local], func(t *testing.T) {
			a, b := pair(t, DefaultSettings())
			id := openStream(t, a, b, false)
			target := a
			if !local {
				target = b
			}
			if _, err := target.Receive(dataPacket(id, "forbidden", false)); !errors.Is(err, ErrProtocol) || !target.Failed() {
				t.Fatal(err)
			}
			if _, err := target.Ping(1); !errors.Is(err, ErrFailed) {
				t.Fatal(err)
			}
		})
	}
}

func TestProtocolFailuresAreTerminal(t *testing.T) {
	tests := map[string]func(*Session, uint32) *pb.Packet{
		"empty data":   func(_ *Session, id uint32) *pb.Packet { return dataPacket(id, "", false) },
		"unknown enum": func(_ *Session, id uint32) *pb.Packet { return resetPacket(id, 99, "") },
		"wrong scope": func(_ *Session, id uint32) *pb.Packet {
			return &pb.Packet{StreamId: id, Body: &pb.Packet_Ping{Ping: &pb.Ping{}}}
		},
		"no body":    func(_ *Session, _ uint32) *pb.Packet { return &pb.Packet{} },
		"nil nested": func(_ *Session, _ uint32) *pb.Packet { return &pb.Packet{Body: &pb.Packet_Ping{}} },
		"duplicate ack": func(_ *Session, id uint32) *pb.Packet {
			return &pb.Packet{StreamId: id, Body: &pb.Packet_OpenAck{OpenAck: &pb.OpenAck{}}}
		},
		"future ack": func(_ *Session, id uint32) *pb.Packet {
			return &pb.Packet{StreamId: id + 2, Body: &pb.Packet_OpenAck{OpenAck: &pb.OpenAck{}}}
		},
		"future reset":        func(_ *Session, id uint32) *pb.Packet { return resetPacket(id+2, 0, "") },
		"future window":       func(_ *Session, id uint32) *pb.Packet { return window(id+2, 1) },
		"future data":         func(_ *Session, id uint32) *pb.Packet { return dataPacket(id+2, "x", false) },
		"zero window":         func(_ *Session, id uint32) *pb.Packet { return window(id, 0) },
		"stream overflow":     func(_ *Session, id uint32) *pb.Packet { return window(id, math.MaxUint32) },
		"connection overflow": func(_ *Session, _ uint32) *pb.Packet { return window(0, 1) },
		"max data":            func(_ *Session, id uint32) *pb.Packet { return dataPacket(id, strings.Repeat("x", 16385), false) },
		"repeated handshake": func(_ *Session, _ uint32) *pb.Packet {
			return &pb.Packet{Body: &pb.Packet_Welcome{Welcome: &pb.Welcome{Version: 1, Settings: DefaultSettings()}}}
		},
		"nil packet": func(_ *Session, _ uint32) *pb.Packet { return nil },
	}
	for name, makePacket := range tests {
		t.Run(name, func(t *testing.T) {
			a, b := pair(t, DefaultSettings())
			id := openStream(t, a, b, true)
			if _, err := a.Receive(makePacket(a, id)); !errors.Is(err, ErrProtocol) || !a.Failed() {
				t.Fatal(err)
			}
			if _, err := a.Receive(window(0, 1)); !errors.Is(err, ErrFailed) {
				t.Fatal(err)
			}
			if _, _, err := a.Open(&pb.Open{Profile: "bytes.v1"}); !errors.Is(err, ErrFailed) {
				t.Fatal(err)
			}
			events := a.Close()
			if len(events) != 1 || events[0].Code != pb.ErrorCode_INTERNAL {
				t.Fatal(events)
			}
		})
	}
}

func TestReceiveWindowLimitsAndFin(t *testing.T) {
	for _, mode := range []string{"stream", "connection", "after fin", "historical connection"} {
		t.Run(mode, func(t *testing.T) {
			settings := DefaultSettings()
			settings.MaxData = 65536
			settings.StreamWindow = 65536
			settings.ConnectionWindow = 65536
			a, b := pair(t, settings)
			id := openStream(t, a, b, true)
			switch mode {
			case "stream":
				b.local.StreamWindow = 4096
				deliver(t, b, dataPacket(id, strings.Repeat("x", 4096), false))
			case "connection":
				deliver(t, b, dataPacket(id, strings.Repeat("x", 65536), false))
				id = openStream(t, a, b, true)
			case "after fin":
				deliver(t, b, dataPacket(id, "", true))
			case "historical connection":
				old := openStream(t, a, b, true)
				if _, err := b.Reset(old, 0, ""); err != nil {
					t.Fatal(err)
				}
				deliver(t, b, dataPacket(id, strings.Repeat("x", 65536), false))
				id = old
			}
			if _, err := b.Receive(dataPacket(id, "x", false)); !errors.Is(err, ErrProtocol) {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenRejectionHighWaterAndParity(t *testing.T) {
	settings := DefaultSettings()
	settings.MaxStreams = 1
	a, b := pair(t, settings)
	openStream(t, a, b, false)
	for _, tc := range []struct {
		id      uint32
		profile string
		code    pb.ErrorCode
	}{{3, "bytes.v1", pb.ErrorCode_RESOURCE_EXHAUSTED}, {5, "other.v1", pb.ErrorCode_UNSUPPORTED}} {
		r := deliver(t, b, &pb.Packet{StreamId: tc.id, Body: &pb.Packet_Open{Open: &pb.Open{Profile: tc.profile}}})
		if r.Outbound[0].GetReset_().Code != tc.code || b.highestRemote != tc.id {
			t.Fatal(r)
		}
	}
	if _, err := b.Receive(&pb.Packet{StreamId: 3, Body: &pb.Packet_Open{Open: &pb.Open{Profile: "bytes.v1"}}}); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	a, b = pair(t, DefaultSettings())
	if id := openStream(t, b, a, true); id != 2 {
		t.Fatal(id)
	}
	a.highestLocal = math.MaxUint32
	if _, _, err := a.Open(&pb.Open{Profile: "bytes.v1"}); !errors.Is(err, ErrState) || a.highestLocal != math.MaxUint32 {
		t.Fatal(err)
	}
}

func TestHandshakeErrors(t *testing.T) {
	for name, p := range map[string]*pb.Packet{
		"settings missing":   {Body: &pb.Packet_Hello{Hello: &pb.Hello{Version: 1}}},
		"settings zero":      {Body: &pb.Packet_Hello{Hello: &pb.Hello{Version: 1, Settings: &pb.Settings{}}}},
		"version":            {Body: &pb.Packet_Hello{Hello: &pb.Hello{Version: 2, Settings: DefaultSettings()}}},
		"profiles duplicate": {Body: &pb.Packet_Hello{Hello: &pb.Hello{Version: 1, Settings: DefaultSettings(), Profiles: []string{"bytes.v1", "bytes.v1"}}}},
		"ping before hello":  {Body: &pb.Packet_Ping{Ping: &pb.Ping{}}},
	} {
		t.Run(name, func(t *testing.T) {
			s := mustSession(t, Responder, DefaultSettings())
			if _, err := s.Receive(p); !errors.Is(err, ErrProtocol) {
				t.Fatal(err)
			}
		})
	}
	s := mustSession(t, Initiator, DefaultSettings())
	if _, err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Receive(&pb.Packet{Body: &pb.Packet_Welcome{Welcome: &pb.Welcome{Version: 1, Settings: DefaultSettings(), Profiles: []string{"invented.v1"}}}}); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestPayloadOwnership(t *testing.T) {
	a, b := pair(t, DefaultSettings())
	metadata := []byte("meta")
	id, p, err := a.Open(&pb.Open{Profile: "bytes.v1", Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	metadata[0] = 'X'
	if string(p.GetOpen().Metadata) != "meta" {
		t.Fatal("metadata alias")
	}
	r := deliver(t, b, p)
	p.GetOpen().Metadata[0] = 'Y'
	if string(r.Events[0].Open.Metadata) != "meta" {
		t.Fatal("open event alias")
	}
	p, err = b.Accept(id)
	if err != nil {
		t.Fatal(err)
	}
	deliver(t, a, p)
	payload := []byte("abc")
	p, err = a.Send(id, payload, false)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if string(p.GetData().Payload) != "abc" {
		t.Fatal("send alias")
	}
	r = deliver(t, b, p)
	p.GetData().Payload[0] = 'Y'
	if !bytes.Equal(r.Events[0].Payload, []byte("abc")) {
		t.Fatal("receive alias")
	}
	if !proto.Equal(a.local, DefaultSettings()) {
		t.Fatal("settings mutation")
	}
}

func TestFailedAPIAndClose(t *testing.T) {
	a, b := pair(t, DefaultSettings())
	id := openStream(t, a, b, true)
	if _, err := a.Receive(nil); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	operations := map[string]func() error{
		"start":   func() error { _, err := a.Start(); return err },
		"open":    func() error { _, _, err := a.Open(nil); return err },
		"accept":  func() error { _, err := a.Accept(id); return err },
		"send":    func() error { _, err := a.Send(id, nil, true); return err },
		"consume": func() error { _, err := a.Consume(id, 0); return err },
		"reset":   func() error { _, err := a.Reset(id, 0, ""); return err },
		"goaway":  func() error { _, err := a.GoAway(0, ""); return err },
		"ping":    func() error { _, err := a.Ping(0); return err },
		"receive": func() error { _, err := a.Receive(nil); return err },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if err := operation(); !errors.Is(err, ErrFailed) {
				t.Fatal(err)
			}
		})
	}
	if events := a.Close(); len(events) != 1 || events[0].Kind != StreamReset || events[0].ID != id {
		t.Fatal(events)
	}
	if events := a.Close(); len(events) != 0 {
		t.Fatal(events)
	}
}
