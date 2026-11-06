package tm

import (
	"bytes"
	"fmt"
	"maps"
	"math"
	"slices"

	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
	"google.golang.org/protobuf/proto"
)

type sessionState uint8

const (
	stateNew sessionState = iota
	stateWaiting
	stateReady
	stateFailed
)

type stream struct {
	local, accepted, localFin, remoteFin bool
	sendCredit, outstanding              uint32
}

// Session is single-owner and has no timers, I/O, queues, or goroutines.
// Use NewSession; the zero value is not usable.
type Session struct {
	role                  Role
	state                 sessionState
	local                 *pb.Settings
	peer                  *pb.Settings
	profiles              []string
	negotiated            []string
	streams               map[uint32]*stream
	connSendCredit        uint32
	connOutstanding       uint32
	highestLocal          uint32
	highestRemote         uint32
	highestAcceptedRemote uint32
	highestAcked          uint32
	draining              bool
	peerGoAwayLast        *uint32
}

func NewSession(role Role, settings *pb.Settings, profiles []string) (*Session, error) {
	if (role != Initiator && role != Responder) || !validSettings(settings) || !validProfiles(profiles) {
		return nil, ErrArgument
	}
	return &Session{role: role, local: proto.Clone(settings).(*pb.Settings), profiles: slices.Clone(profiles), streams: make(map[uint32]*stream)}, nil
}

func (s *Session) ready() error {
	if s.state == stateFailed {
		return ErrFailed
	}
	if s.state != stateReady {
		return ErrState
	}
	return nil
}

func (s *Session) Failed() bool       { return s.state == stateFailed }
func (s *Session) IsReady() bool      { return s.state == stateReady }
func (s *Session) Draining() bool     { return s.draining }
func (s *Session) Profiles() []string { return slices.Clone(s.negotiated) }

// PeerSettings returns a defensive copy of the peer's receive limits, or nil
// before negotiation. Use MaxData to split application writes explicitly.
func (s *Session) PeerSettings() *pb.Settings {
	if s.peer == nil {
		return nil
	}
	return proto.Clone(s.peer).(*pb.Settings)
}

// NegotiatedProfiles returns an independent copy of the negotiated profile set.
func (s *Session) NegotiatedProfiles() []string { return slices.Clone(s.negotiated) }

func (s *Session) Start() (*pb.Packet, error) {
	if s.state == stateFailed {
		return nil, ErrFailed
	}
	if s.state != stateNew || s.role != Initiator {
		return nil, ErrState
	}
	s.state = stateWaiting
	return &pb.Packet{Body: &pb.Packet_Hello{Hello: &pb.Hello{Version: 1, Settings: proto.Clone(s.local).(*pb.Settings), Profiles: slices.Clone(s.profiles)}}}, nil
}

func (s *Session) Open(o *pb.Open) (uint32, *pb.Packet, error) {
	if err := s.ready(); err != nil {
		return 0, nil, err
	}
	if s.draining {
		return 0, nil, ErrState
	}
	if !validOpen(o) || !slices.Contains(s.negotiated, o.Profile) {
		return 0, nil, ErrArgument
	}
	if s.count(true) >= s.peer.MaxStreams {
		return 0, nil, ErrWouldBlock
	}
	next := uint64(s.highestLocal) + 2
	if s.highestLocal == 0 && s.role == Initiator {
		next = 1
	}
	if next > math.MaxUint32 {
		return 0, nil, ErrState
	}
	id := uint32(next)
	p := &pb.Packet{StreamId: id, Body: &pb.Packet_Open{Open: proto.Clone(o).(*pb.Open)}}
	if err := validatePacket(p); err != nil {
		return 0, nil, ErrArgument
	}
	s.highestLocal = id
	s.streams[id] = &stream{local: true, sendCredit: s.peer.StreamWindow}
	return id, p, nil
}

func (s *Session) Accept(id uint32) (*pb.Packet, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if s.draining {
		return nil, ErrState
	}
	st := s.streams[id]
	if st == nil || st.local || st.accepted {
		return nil, ErrArgument
	}
	st.accepted = true
	s.highestAcceptedRemote = max(s.highestAcceptedRemote, id)
	return &pb.Packet{StreamId: id, Body: &pb.Packet_OpenAck{OpenAck: &pb.OpenAck{}}}, nil
}

func (s *Session) Send(id uint32, payload []byte, fin bool) (*pb.Packet, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	st := s.streams[id]
	if st == nil || !st.accepted || st.localFin {
		return nil, ErrState
	}
	if len(payload) > int(s.peer.MaxData) || (len(payload) == 0 && !fin) {
		return nil, ErrArgument
	}
	n := uint32(len(payload))
	if n > st.sendCredit || n > s.connSendCredit {
		return nil, ErrWouldBlock
	}
	p := &pb.Packet{StreamId: id, Body: &pb.Packet_Data{Data: &pb.Data{Payload: bytes.Clone(payload), Fin: fin}}}
	st.sendCredit -= n
	s.connSendCredit -= n
	st.localFin = fin
	s.reap(id, st)
	return p, nil
}

func (s *Session) Consume(id, n uint32) ([]*pb.Packet, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	st := s.streams[id]
	if st == nil || n > st.outstanding {
		return nil, ErrArgument
	}
	if n == 0 {
		return nil, nil
	}
	st.outstanding -= n
	s.connOutstanding -= n
	out := []*pb.Packet{window(id, n), window(0, n)}
	s.reap(id, st)
	return out, nil
}

func (s *Session) Reset(id uint32, code pb.ErrorCode, reason string) ([]*pb.Packet, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if s.streams[id] == nil || !validCode(code) || !validReason(reason) {
		return nil, ErrArgument
	}
	p := resetPacket(id, code, reason)
	if err := validatePacket(p); err != nil {
		return nil, ErrArgument
	}
	return append([]*pb.Packet{p}, s.drop(id)...), nil
}

func (s *Session) GoAway(code pb.ErrorCode, reason string) ([]*pb.Packet, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if !validCode(code) || !validReason(reason) {
		return nil, ErrArgument
	}
	last := s.highestAcceptedRemote
	p := &pb.Packet{Body: &pb.Packet_GoAway{GoAway: &pb.GoAway{LastStreamId: last, Code: code, Reason: reason}}}
	if err := validatePacket(p); err != nil {
		return nil, ErrArgument
	}
	s.draining = true
	var out []*pb.Packet
	for _, id := range s.pending(false) {
		out = append(out, resetPacket(id, pb.ErrorCode_REFUSED, ""))
		out = append(out, s.drop(id)...)
	}
	return append(out, p), nil
}

func (s *Session) Ping(token uint64) (*pb.Packet, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	return &pb.Packet{Body: &pb.Packet_Ping{Ping: &pb.Ping{Token: token}}}, nil
}

// Close marks the session failed and notifies every still-active stream. It is
// also the cleanup operation after a terminal Receive/framing/transport error.
func (s *Session) Close() []Event {
	s.state = stateFailed
	ids := slices.Sorted(maps.Keys(s.streams))
	events := make([]Event, 0, len(ids))
	for _, id := range ids {
		events = append(events, Event{Kind: StreamReset, ID: id, Code: pb.ErrorCode_INTERNAL, Reason: "transport closed"})
	}
	clear(s.streams)
	s.connOutstanding = 0
	return events
}

func (s *Session) count(local bool) uint32 {
	var n uint32
	for _, st := range s.streams {
		if st.local == local {
			n++
		}
	}
	return n
}
func (s *Session) pending(local bool) []uint32 {
	var ids []uint32
	for id, st := range s.streams {
		if st.local == local && !st.accepted {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}
func (s *Session) localID(id uint32) bool { return (id&1 == 1) == (s.role == Initiator) }
func (s *Session) historical(id uint32) bool {
	if id == 0 {
		return false
	}
	if s.localID(id) {
		return id <= s.highestLocal
	}
	return id <= s.highestRemote
}
func (s *Session) reap(id uint32, st *stream) {
	if st.localFin && st.remoteFin && st.outstanding == 0 {
		delete(s.streams, id)
	}
}
func (s *Session) drop(id uint32) []*pb.Packet {
	st := s.streams[id]
	delete(s.streams, id)
	if st.outstanding == 0 {
		return nil
	}
	s.connOutstanding -= st.outstanding
	return []*pb.Packet{window(0, st.outstanding)}
}

// Receive applies one packet atomically on valid input. Any protocol error is
// terminal; call Close to obtain failure events for the remaining streams.
func (s *Session) Receive(p *pb.Packet) (Result, error) {
	if s.state == stateFailed {
		return Result{}, ErrFailed
	}
	r, err := s.receive(p)
	if err != nil {
		s.state = stateFailed
		return Result{}, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	return r, nil
}

func (s *Session) receive(p *pb.Packet) (Result, error) {
	if err := validatePacket(p); err != nil {
		return Result{}, err
	}
	if s.state != stateReady {
		return s.handshake(p)
	}
	id := p.StreamId
	var r Result
	switch v := p.Body.(type) {
	case *pb.Packet_Open:
		if s.localID(id) || id <= s.highestRemote {
			return r, ErrProtocol
		}
		s.highestRemote = id
		code := pb.ErrorCode_CANCELLED
		switch {
		case s.draining:
			code = pb.ErrorCode_REFUSED
		case !slices.Contains(s.negotiated, v.Open.Profile):
			code = pb.ErrorCode_UNSUPPORTED
		case s.count(false) >= s.local.MaxStreams:
			code = pb.ErrorCode_RESOURCE_EXHAUSTED
		default:
			s.streams[id] = &stream{sendCredit: s.peer.StreamWindow}
			r.Events = []Event{{Kind: Opened, ID: id, Open: proto.Clone(v.Open).(*pb.Open)}}
			return r, nil
		}
		r.Outbound = []*pb.Packet{resetPacket(id, code, "")}
	case *pb.Packet_Ping:
		r.Outbound = []*pb.Packet{{Body: &pb.Packet_Pong{Pong: &pb.Pong{Token: v.Ping.Token}}}}
	case *pb.Packet_Pong:
		r.Events = []Event{{Kind: ReceivedPong, Token: v.Pong.Token}}
	case *pb.Packet_GoAway:
		g := v.GoAway
		last := g.LastStreamId
		if (last != 0 && (!s.localID(last) || last > s.highestLocal)) || last < s.highestAcked || (s.peerGoAwayLast != nil && last > *s.peerGoAwayLast) {
			return r, ErrProtocol
		}
		s.peerGoAwayLast = &last
		s.draining = true
		for _, id := range s.pending(true) {
			r.Outbound = append(r.Outbound, s.drop(id)...)
			r.Events = append(r.Events, Event{Kind: StreamReset, ID: id, Code: pb.ErrorCode_REFUSED})
		}
		for _, id := range s.pending(false) {
			r.Outbound = append(r.Outbound, resetPacket(id, pb.ErrorCode_REFUSED, ""))
			r.Outbound = append(r.Outbound, s.drop(id)...)
			r.Events = append(r.Events, Event{Kind: StreamReset, ID: id, Code: pb.ErrorCode_REFUSED})
		}
		r.Events = append(r.Events, Event{Kind: ReceivedGoAway, LastStreamID: last, Code: g.Code, Reason: g.Reason})
	case *pb.Packet_Window:
		n := v.Window.Credit
		if id == 0 {
			if n > s.peer.ConnectionWindow-s.connSendCredit {
				return r, ErrProtocol
			}
			s.connSendCredit += n
			return r, nil
		}
		st := s.streams[id]
		if st == nil {
			if !s.historical(id) {
				return r, ErrProtocol
			}
			return r, nil
		}
		if n > s.peer.StreamWindow-st.sendCredit {
			return r, ErrProtocol
		}
		st.sendCredit += n
	case *pb.Packet_OpenAck:
		st := s.streams[id]
		if st == nil {
			if !s.historical(id) {
				return r, ErrProtocol
			}
			return r, nil
		}
		if !st.local || st.accepted {
			return r, ErrProtocol
		}
		st.accepted = true
		s.highestAcked = max(s.highestAcked, id)
		r.Events = []Event{{Kind: Accepted, ID: id}}
	case *pb.Packet_Reset_:
		if s.streams[id] == nil {
			if !s.historical(id) {
				return r, ErrProtocol
			}
			return r, nil
		}
		r.Outbound = s.drop(id)
		r.Events = []Event{{Kind: StreamReset, ID: id, Code: v.Reset_.Code, Reason: v.Reset_.Reason}}
	case *pb.Packet_Data:
		d := v.Data
		n := uint32(len(d.Payload))
		if n > s.local.MaxData || n > s.local.ConnectionWindow-s.connOutstanding {
			return r, ErrProtocol
		}
		st := s.streams[id]
		if st == nil {
			if !s.historical(id) {
				return r, ErrProtocol
			}
			if n > 0 {
				r.Outbound = []*pb.Packet{window(0, n)}
			}
			return r, nil
		}
		if !st.accepted || st.remoteFin || n > s.local.StreamWindow-st.outstanding {
			return r, ErrProtocol
		}
		st.outstanding += n
		s.connOutstanding += n
		st.remoteFin = d.Fin
		r.Events = []Event{{Kind: ReceivedData, ID: id, Payload: bytes.Clone(d.Payload), Fin: d.Fin}}
		s.reap(id, st)
	default:
		return r, ErrProtocol
	}
	return r, nil
}

func (s *Session) handshake(p *pb.Packet) (Result, error) {
	var r Result
	var settings *pb.Settings
	var profiles []string
	var version uint32
	switch v := p.Body.(type) {
	case *pb.Packet_Hello:
		if s.role != Responder || s.state != stateNew {
			return r, ErrProtocol
		}
		settings, profiles, version = v.Hello.Settings, v.Hello.Profiles, v.Hello.Version
	case *pb.Packet_Welcome:
		if s.role != Initiator || s.state != stateWaiting {
			return r, ErrProtocol
		}
		settings, profiles, version = v.Welcome.Settings, v.Welcome.Profiles, v.Welcome.Version
	default:
		return r, ErrProtocol
	}
	if version != 1 || !validSettings(settings) || !validProfiles(profiles) {
		return r, ErrProtocol
	}
	negotiated := make([]string, 0, len(profiles))
	for _, p := range profiles {
		if slices.Contains(s.profiles, p) {
			negotiated = append(negotiated, p)
		} else if s.role == Initiator {
			return r, ErrProtocol
		}
	}
	s.peer = proto.Clone(settings).(*pb.Settings)
	s.connSendCredit = settings.ConnectionWindow
	s.negotiated = negotiated
	s.state = stateReady
	if s.role == Responder {
		r.Outbound = []*pb.Packet{{Body: &pb.Packet_Welcome{Welcome: &pb.Welcome{Version: 1, Settings: proto.Clone(s.local).(*pb.Settings), Profiles: slices.Clone(negotiated)}}}}
	}
	r.Events = []Event{{Kind: Ready}}
	return r, nil
}
