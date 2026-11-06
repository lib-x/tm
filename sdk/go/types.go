// Package tm implements the tmp.pb.v1 single-owner, Sans-I/O protocol.
// Callers must serialize all operations and transmit returned packets in order.
package tm

import (
	"errors"
	"fmt"
	"regexp"
	"unicode/utf8"

	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
	"google.golang.org/protobuf/proto"
)

const Version = "0.1.0"

var (
	ErrFailed     = errors.New("tm: session failed")
	ErrState      = errors.New("tm: invalid state")
	ErrArgument   = errors.New("tm: invalid argument")
	ErrWouldBlock = errors.New("tm: insufficient credit or stream capacity")
	ErrProtocol   = errors.New("tm: protocol error")
	ErrFraming    = errors.New("tm: invalid frame")
)

type Role uint8

const (
	Initiator Role = iota
	Responder
)

type EventKind uint8

const (
	Ready EventKind = iota
	Opened
	Accepted
	ReceivedData
	StreamReset
	ReceivedPong
	ReceivedGoAway
)

// Event owns Open and Payload; modifying them never changes Session state.
type Event struct {
	Kind         EventKind
	ID           uint32
	Open         *pb.Open
	Payload      []byte
	Fin          bool
	Code         pb.ErrorCode
	Reason       string
	Token        uint64
	LastStreamID uint32
}

type Result struct {
	Outbound []*pb.Packet
	Events   []Event
}

// DefaultSettings returns a fresh receiver configuration.
func DefaultSettings() *pb.Settings {
	return &pb.Settings{MaxData: 16384, StreamWindow: 65536, ConnectionWindow: 1048576, MaxStreams: 256}
}

func validSettings(v *pb.Settings) bool {
	return v != nil && v.MaxData >= 1024 && v.MaxData <= 65536 && v.StreamWindow >= 4096 && v.StreamWindow <= 16777216 && v.ConnectionWindow >= 65536 && v.ConnectionWindow <= 268435456 && v.MaxStreams >= 1 && v.MaxStreams <= 4096
}

var profilePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]{0,63}$`)

func validProfiles(profiles []string) bool {
	if len(profiles) > 32 {
		return false
	}
	seen := make(map[string]bool, len(profiles))
	for _, p := range profiles {
		if !profilePattern.MatchString(p) || seen[p] {
			return false
		}
		seen[p] = true
	}
	return true
}
func validOpen(v *pb.Open) bool {
	return v != nil && profilePattern.MatchString(v.Profile) && utf8.ValidString(v.Route) && len(v.Route) <= 256 && len(v.Metadata) <= 4096 && v.Class >= pb.Class_INTERACTIVE && v.Class <= pb.Class_BULK
}
func validCode(c pb.ErrorCode) bool { return c >= pb.ErrorCode_CANCELLED && c <= pb.ErrorCode_INTERNAL }

func validReason(reason string) bool { return len(reason) <= 256 && utf8.ValidString(reason) }

// validatePacket also protects callers using generated structs directly.
func validatePacket(p *pb.Packet) error {
	if p == nil {
		return ErrProtocol
	}
	m := p.ProtoReflect()
	f := m.WhichOneof(m.Descriptor().Oneofs().ByName("body"))
	if f == nil || !m.Get(f).Message().IsValid() || len(m.GetUnknown()) != 0 {
		return ErrProtocol
	}
	link := false
	switch v := p.Body.(type) {
	case *pb.Packet_Hello:
		link = true
	case *pb.Packet_Welcome:
		link = true
	case *pb.Packet_Ping:
		link = true
	case *pb.Packet_Pong:
		link = true
	case *pb.Packet_GoAway:
		link = true
		if !validCode(v.GoAway.Code) || !validReason(v.GoAway.Reason) {
			return ErrProtocol
		}
	case *pb.Packet_Open:
		if !validOpen(v.Open) {
			return ErrProtocol
		}
	case *pb.Packet_Reset_:
		if !validCode(v.Reset_.Code) || !validReason(v.Reset_.Reason) {
			return ErrProtocol
		}
	case *pb.Packet_Data:
		if len(v.Data.Payload) == 0 && !v.Data.Fin {
			return ErrProtocol
		}
	case *pb.Packet_Window:
		if v.Window.Credit == 0 {
			return ErrProtocol
		}
		return marshalCheck(p)
	case *pb.Packet_OpenAck:
	default:
		return ErrProtocol
	}
	if link != (p.StreamId == 0) {
		return ErrProtocol
	}
	return marshalCheck(p)
}
func marshalCheck(p *pb.Packet) error {
	if size := proto.Size(p); size == 0 || size > MaxPacket {
		return fmt.Errorf("%w: packet size", ErrProtocol)
	}
	b, err := proto.Marshal(p)
	if err != nil || len(b) == 0 || len(b) > MaxPacket {
		return fmt.Errorf("%w: invalid protobuf or packet size", ErrProtocol)
	}
	return nil
}

func window(id, n uint32) *pb.Packet {
	return &pb.Packet{StreamId: id, Body: &pb.Packet_Window{Window: &pb.Window{Credit: n}}}
}
func resetPacket(id uint32, code pb.ErrorCode, reason string) *pb.Packet {
	return &pb.Packet{StreamId: id, Body: &pb.Packet_Reset_{Reset_: &pb.Reset{Code: code, Reason: reason}}}
}
