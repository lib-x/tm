// Package conformance implements the line-oriented interoperability harness.
package conformance

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	tm "github.com/lib-x/tm/sdk/go"
	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
	"google.golang.org/protobuf/proto"
)

func Execute() {
	if err := Run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func transcript() []*pb.Packet {
	return []*pb.Packet{
		{Body: &pb.Packet_Hello{Hello: &pb.Hello{Version: 1, Settings: tm.DefaultSettings(), Profiles: []string{"bytes.v1"}}}},
		{Body: &pb.Packet_Welcome{Welcome: &pb.Welcome{Version: 1, Settings: tm.DefaultSettings(), Profiles: []string{"bytes.v1"}}}},
		{StreamId: 1, Body: &pb.Packet_Open{Open: &pb.Open{Profile: "bytes.v1", Route: "echo", Class: pb.Class_INTERACTIVE}}},
		{StreamId: 1, Body: &pb.Packet_OpenAck{OpenAck: &pb.OpenAck{}}},
		{StreamId: 1, Body: &pb.Packet_Data{Data: &pb.Data{Payload: []byte("你好 TMP"), Fin: true}}},
		{StreamId: 1, Body: &pb.Packet_Data{Data: &pb.Data{Payload: []byte("ok"), Fin: true}}},
		{StreamId: 1, Body: &pb.Packet_Window{Window: &pb.Window{Credit: 10}}},
		{Body: &pb.Packet_Window{Window: &pb.Window{Credit: 10}}},
		{StreamId: 1, Body: &pb.Packet_Window{Window: &pb.Window{Credit: 2}}},
		{Body: &pb.Packet_Window{Window: &pb.Window{Credit: 2}}},
	}
}

func scanner(r io.Reader) *bufio.Scanner {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 4096), 2*(tm.MaxPacket+4)+2)
	return s
}

func readLine(line string) (*pb.Packet, error) {
	b, err := hex.DecodeString(line)
	if err != nil {
		return nil, err
	}
	return tm.Decode(b)
}

func writePackets(w *bufio.Writer, ps ...*pb.Packet) error {
	for _, p := range ps {
		b, err := tm.Encode(p)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, hex.EncodeToString(b)); err != nil {
			return err
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func Run(args []string, r io.Reader, w io.Writer) error {
	if len(args) == 1 {
		switch args[0] {
		case "emit":
			return writePackets(bufio.NewWriter(w), transcript()...)
		case "verify":
			return verify(r)
		}
	}
	if len(args) == 2 && args[0] == "endpoint" {
		switch args[1] {
		case "initiator":
			return endpoint(tm.Initiator, r, w)
		case "responder":
			return endpoint(tm.Responder, r, w)
		}
	}
	return fmt.Errorf("usage: conformance emit | verify | endpoint initiator|responder")
}

// Semantic comparison uses decoded known fields, ignoring compatible unknown
// nested fields and accepting any protobuf field ordering.
func verify(r io.Reader) error {
	s := scanner(r)
	expected := transcript()
	i := 0
	for s.Scan() {
		if i >= len(expected) {
			return fmt.Errorf("extra transcript line")
		}
		p, err := readLine(s.Text())
		if err != nil {
			return fmt.Errorf("line %d: %w", i+1, err)
		}
		clearUnknown(p.ProtoReflect())
		if !proto.Equal(p, expected[i]) {
			return fmt.Errorf("line %d: transcript semantic mismatch", i+1)
		}
		i++
	}
	if err := s.Err(); err != nil {
		return err
	}
	if i != len(expected) {
		return fmt.Errorf("expected %d lines, got %d", len(expected), i)
	}
	return nil
}

func endpoint(role tm.Role, r io.Reader, w io.Writer) error {
	s, err := tm.NewSession(role, tm.DefaultSettings(), []string{"bytes.v1"})
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(w)
	if role == tm.Initiator {
		p, err := s.Start()
		if err != nil {
			return err
		}
		if err := writePackets(writer, p); err != nil {
			return err
		}
	}
	scan := scanner(r)
	var streamID uint32
	gotData := false
	for scan.Scan() {
		p, err := readLine(scan.Text())
		if err != nil {
			return err
		}
		result, err := s.Receive(p)
		if err != nil {
			return err
		}
		if err := writePackets(writer, result.Outbound...); err != nil {
			return err
		}
		for _, event := range result.Events {
			switch event.Kind {
			case tm.Ready:
				if role == tm.Initiator {
					id, p, err := s.Open(&pb.Open{Profile: "bytes.v1", Route: "echo", Class: pb.Class_INTERACTIVE})
					if err != nil {
						return err
					}
					streamID = id
					if err := writePackets(writer, p); err != nil {
						return err
					}
				}
			case tm.Opened:
				if role != tm.Responder || streamID != 0 || event.Open.Profile != "bytes.v1" || event.Open.Route != "echo" || event.Open.Class != pb.Class_INTERACTIVE {
					return fmt.Errorf("unexpected OPEN")
				}
				streamID = event.ID
				p, err := s.Accept(event.ID)
				if err != nil {
					return err
				}
				if err := writePackets(writer, p); err != nil {
					return err
				}
			case tm.Accepted:
				if role != tm.Initiator || event.ID != streamID {
					return fmt.Errorf("unexpected ACK")
				}
				p, err := s.Send(event.ID, []byte("你好 TMP"), true)
				if err != nil {
					return err
				}
				if err := writePackets(writer, p); err != nil {
					return err
				}
			case tm.ReceivedData:
				want := "你好 TMP"
				if role == tm.Initiator {
					want = "ok"
				}
				if gotData || event.ID != streamID || string(event.Payload) != want || !event.Fin {
					return fmt.Errorf("unexpected DATA")
				}
				gotData = true
				ps, err := s.Consume(event.ID, uint32(len(event.Payload)))
				if err != nil {
					return err
				}
				if err := writePackets(writer, ps...); err != nil {
					return err
				}
				if role == tm.Responder {
					p, err := s.Send(event.ID, []byte("ok"), true)
					if err != nil {
						return err
					}
					if err := writePackets(writer, p); err != nil {
						return err
					}
				} else {
					ps, err := s.GoAway(pb.ErrorCode_CANCELLED, "")
					if err != nil {
						return err
					}
					if err := writePackets(writer, ps...); err != nil {
						return err
					}
				}
			case tm.ReceivedGoAway:
				if !gotData {
					return fmt.Errorf("GOAWAY before exchange")
				}
				if role == tm.Responder {
					ps, err := s.GoAway(pb.ErrorCode_CANCELLED, "")
					if err != nil {
						return err
					}
					if err := writePackets(writer, ps...); err != nil {
						return err
					}
				}
				return nil
			case tm.StreamReset:
				return fmt.Errorf("unexpected RESET for %d", event.ID)
			}
		}
	}
	if err := scan.Err(); err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}
