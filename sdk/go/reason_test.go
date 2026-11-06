package tm

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
	"google.golang.org/protobuf/proto"
)

var reasonCases = []struct {
	name   string
	reason string
	valid  bool
}{
	{"empty", "", true},
	{"ascii256", strings.Repeat("x", 256), true},
	{"ascii257", strings.Repeat("x", 257), false},
	{"utf8_256", strings.Repeat("你", 85) + "a", true},
	{"utf8_257", strings.Repeat("你", 85) + "ab", false},
	{"invalid_utf8", string([]byte{255}), false},
}

func TestReasonLocalBoundariesAndAtomicity(t *testing.T) {
	for _, operation := range []string{"reset", "goaway"} {
		for _, tc := range reasonCases {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				a, b := pair(t, DefaultSettings())
				id := openStream(t, a, b, true)
				sendData(t, b, a, id, "unread", false)
				openStream(t, b, a, false)
				before := *a
				before.streams = make(map[uint32]*stream, len(a.streams))
				for id, st := range a.streams {
					clone := *st
					before.streams[id] = &clone
				}
				var packets []*pb.Packet
				var err error
				if operation == "reset" {
					packets, err = a.Reset(id, pb.ErrorCode_CANCELLED, tc.reason)
				} else {
					packets, err = a.GoAway(pb.ErrorCode_CANCELLED, tc.reason)
				}
				if !tc.valid {
					if !errors.Is(err, ErrArgument) {
						t.Fatalf("expected ErrArgument, got %v", err)
					}
					if len(packets) != 0 || !reflect.DeepEqual(a, &before) {
						t.Fatal("invalid reason mutated session or emitted packets")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if operation == "reset" {
					if len(packets) != 2 || packets[0].GetReset_().Reason != tc.reason || packets[1].GetWindow().Credit != 6 {
						t.Fatal(packets)
					}
				} else {
					if len(packets) != 2 || packets[0].GetReset_() == nil || packets[1].GetGoAway().Reason != tc.reason {
						t.Fatal(packets)
					}
				}
			})
		}
	}
}

func TestReasonReceiveBoundaries(t *testing.T) {
	for _, kind := range []string{"reset", "historical_reset", "goaway"} {
		for _, tc := range reasonCases {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				a, b := pair(t, DefaultSettings())
				id := openStream(t, a, b, true)
				if kind == "historical_reset" {
					if _, err := a.Reset(id, 0, ""); err != nil {
						t.Fatal(err)
					}
				}
				p := resetPacket(id, pb.ErrorCode_CANCELLED, tc.reason)
				if kind == "goaway" {
					p = goAwayPacket(id)
					p.GetGoAway().Reason = tc.reason
				}
				validationErr := validatePacket(p)
				result, err := a.Receive(p)
				if !tc.valid {
					if !errors.Is(validationErr, ErrProtocol) || !errors.Is(err, ErrProtocol) || !a.Failed() {
						t.Fatalf("validation=%v receive=%v failed=%v", validationErr, err, a.Failed())
					}
					return
				}
				if validationErr != nil || err != nil {
					t.Fatal(validationErr, err)
				}
				if kind == "historical_reset" {
					if len(result.Events) != 0 || len(result.Outbound) != 0 {
						t.Fatal(result)
					}
				} else if len(result.Events) != 1 || result.Events[0].Reason != tc.reason {
					t.Fatal(result)
				}
			})
		}
	}
}

func TestReasonFramingBoundaries(t *testing.T) {
	for _, kind := range []string{"reset", "goaway"} {
		for _, tc := range reasonCases {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				p := resetPacket(1, 0, tc.reason)
				if kind == "goaway" {
					p = goAwayPacket(1)
					p.GetGoAway().Reason = tc.reason
				}
				_, encodeErr := Encode(p)
				if tc.valid && encodeErr != nil {
					t.Fatal(encodeErr)
				}
				if !tc.valid && encodeErr == nil {
					t.Fatal("Encode accepted invalid reason")
				}
				if tc.name == "invalid_utf8" {
					return
				} // Protobuf marshal itself rejects invalid strings.
				wire, err := proto.Marshal(p)
				if err != nil {
					t.Fatal(err)
				}
				_, decodeErr := Decode(frame(wire))
				if tc.valid && decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if !tc.valid && !errors.Is(decodeErr, ErrFraming) {
					t.Fatalf("Decode accepted invalid reason: %v", decodeErr)
				}
			})
		}
	}
}
