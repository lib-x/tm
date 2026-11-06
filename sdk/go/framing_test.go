package tm

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func frame(body []byte) []byte {
	b := make([]byte, len(body)+4)
	binary.BigEndian.PutUint32(b, uint32(len(body)))
	copy(b[4:], body)
	return b
}
func mustEncode(t *testing.T, p *pb.Packet) []byte {
	t.Helper()
	b, err := Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestGoldenFraming(t *testing.T) {
	// fixed64 is little endian inside protobuf, while framing is big endian.
	p := &pb.Packet{Body: &pb.Packet_Ping{Ping: &pb.Ping{Token: 0x0102030405060708}}}
	b := mustEncode(t, p)
	if hex.EncodeToString(b) != "0000000c8a0109090807060504030201" {
		t.Fatal(hex.EncodeToString(b))
	}
	got, err := Decode(b)
	if err != nil || !proto.Equal(got, p) {
		t.Fatal(got, err)
	}
}

func TestStrictTopLevelScan(t *testing.T) {
	valid := []byte{0x08, 1, 0x6a, 0} // stream=1, OpenAck{}
	bad := map[string][]byte{
		"missing body":       {8, 1},
		"duplicate stream":   {8, 1, 8, 1, 0x6a, 0},
		"same body twice":    {8, 1, 0x6a, 0, 0x6a, 0},
		"different bodies":   {8, 1, 0x6a, 0, 0x72, 2, 0x10, 1},
		"unknown tag":        {8, 1, 0x6a, 0, 0x10, 1},
		"unknown body":       {8, 1, 0xa2, 1, 0},
		"wrong stream wire":  {0xa, 1, 0, 0x6a, 0},
		"wrong body wire":    {8, 1, 0x68, 0},
		"uint32 overflow":    {8, 0x80, 0x80, 0x80, 0x80, 0x10, 0x6a, 0},
		"varint overflow":    {8, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 2, 0x6a, 0},
		"truncated body":     {8, 1, 0x6a, 3, 0},
		"bad inner protobuf": {8, 1, 0x6a, 1, 255},
		"invalid utf8":       {8, 1, 0x62, 3, 0xa, 1, 255},
		"unknown class enum": {8, 1, 0x62, 5, 0xa, 1, 'x', 0x18, 99},
		"zero tag":           {8, 1, 0x6a, 0, 0},
	}
	if _, err := Decode(frame(valid)); err != nil {
		t.Fatal(err)
	}
	for name, b := range bad {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(frame(b)); err == nil {
				t.Fatalf("accepted %x", b)
			}
		})
	}
	for name, b := range map[string][]byte{"zero length": {0, 0, 0, 0}, "oversize": {0, 2, 0, 1}, "partial prefix": {0, 0}, "extra bytes": append(frame(valid), 0), "short body": {0, 0, 0, 2, 0}} {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(b); err == nil {
				t.Fatal("accepted bad length")
			}
		})
	}
}

func TestNestedUnknownAndDuplicateFields(t *testing.T) {
	// Nested scalars use protobuf last-value rules; unknown fields are retained.
	body := []byte{0x08, 1, 0x7a, 6, 0x08, 1, 0x08, 2, 0x10, 9}
	p, err := Decode(frame(body))
	if err != nil || p.GetWindow().Credit != 2 {
		t.Fatal(p, err)
	}
	// The stream id may follow the body and may be explicitly zero for links.
	if _, err := Decode(frame([]byte{0x8a, 1, 0, 8, 0})); err != nil {
		t.Fatal(err)
	}
	// Repeated nested messages merge, unlike Packet's strictly singular body.
	hello := &pb.Packet{Body: &pb.Packet_Hello{Hello: &pb.Hello{Version: 1, Settings: DefaultSettings()}}}
	b := mustEncode(t, hello)
	if _, err := Decode(b); err != nil {
		t.Fatal(err)
	}
}

func TestDecoderEverySplitAndBoundedDelivery(t *testing.T) {
	p := dataPacket(1, "hello", true)
	encoded := mustEncode(t, p)
	all := append(bytes.Clone(encoded), encoded...)
	for split := range len(all) + 1 {
		var d Decoder
		count := 0
		yield := func(got *pb.Packet) error {
			count++
			if !proto.Equal(got, p) {
				t.Fatal(got)
			}
			return nil
		}
		if err := d.Feed(all[:split], yield); err != nil {
			t.Fatal(split, err)
		}
		if err := d.Feed(all[split:], yield); err != nil {
			t.Fatal(split, err)
		}
		if err := d.Finish(); err != nil || count != 2 {
			t.Fatal(split, count, err)
		}
	}
	var d Decoder
	count := 0
	for _, v := range all {
		if err := d.Feed([]byte{v}, func(*pb.Packet) error { count++; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if count != 2 || d.body != nil || d.headerN != 0 {
		t.Fatal("decoder retained delivered frame")
	}
	many := bytes.Repeat(encoded, 10000)
	count = 0
	if err := d.Feed(many, func(*pb.Packet) error {
		count++
		if cap(d.body) > MaxPacket {
			t.Fatal("unbounded")
		}
		return nil
	}); err != nil || count != 10000 {
		t.Fatal(count, err)
	}
}

func TestDecoderTerminalErrors(t *testing.T) {
	b := mustEncode(t, dataPacket(1, "hello", false))
	for cut := 1; cut < len(b); cut++ {
		var d Decoder
		if err := d.Feed(b[:cut], func(*pb.Packet) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := d.Finish(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal(cut, err)
		}
		if err := d.Feed(b, func(*pb.Packet) error { return nil }); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal(err)
		}
	}
	var d Decoder
	if err := d.Feed([]byte{0, 2, 0, 1}, func(*pb.Packet) error { return nil }); !errors.Is(err, ErrFraming) || d.body != nil {
		t.Fatal(err)
	}
	if err := d.Feed(b, func(*pb.Packet) error { return nil }); !errors.Is(err, ErrFraming) {
		t.Fatal(err)
	}
	d = Decoder{}
	stop := errors.New("callback stop")
	calls := 0
	if err := d.Feed(append(bytes.Clone(b), b...), func(*pb.Packet) error { calls++; return stop }); !errors.Is(err, stop) || calls != 1 {
		t.Fatal(calls, err)
	}
	if err := d.Finish(); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	d = Decoder{}
	if err := d.Feed(b, nil); !errors.Is(err, ErrArgument) {
		t.Fatal(err)
	}
	if err := d.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := d.Feed(b, func(*pb.Packet) error { return nil }); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
}

func TestMaximumPacketAndBeforeAllocation(t *testing.T) {
	// Fill a nested unknown field to reach exactly MaxPacket bytes.
	body := protowire.AppendTag(nil, 100, protowire.BytesType)
	body = protowire.AppendBytes(body, make([]byte, MaxPacket-10))
	packet := protowire.AppendTag(nil, 17, protowire.BytesType)
	packet = protowire.AppendBytes(packet, body)
	if len(packet) != MaxPacket {
		t.Fatal(len(packet))
	}
	if _, err := Decode(frame(packet)); err != nil {
		t.Fatal(err)
	}
	var d Decoder
	if err := d.Feed([]byte{0, 2, 0, 0}, func(*pb.Packet) error { return nil }); err != nil || len(d.body) != MaxPacket {
		t.Fatal(err)
	}
	if err := d.Finish(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}

func FuzzDecodeAndIncremental(f *testing.F) {
	f.Add([]byte{0, 0, 0, 4, 8, 1, 0x6a, 0})
	f.Add([]byte{0, 2, 0, 1})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxPacket+4 {
			return
		}
		var d Decoder
		err := d.Feed(b, func(p *pb.Packet) error {
			encoded, err := Encode(p)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Decode(encoded); err != nil {
				t.Fatal(err)
			}
			return nil
		})
		if len(d.body) > MaxPacket {
			t.Fatal("unbounded decoder")
		}
		if err == nil {
			_ = d.Finish()
		}
	})
}
