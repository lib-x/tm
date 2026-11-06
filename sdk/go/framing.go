package tm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const MaxPacket = 131072

// Encode encodes one complete length-prefixed packet.
func Encode(p *pb.Packet) ([]byte, error) {
	if err := validatePacket(p); err != nil {
		return nil, err
	}
	b, err := proto.Marshal(p)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(out, uint32(len(b)))
	copy(out[4:], b)
	return out, nil
}

// Decode accepts exactly one framed packet. The strict top-level scan happens
// before generated protobuf decoding; nested unknown fields remain compatible.
func Decode(frame []byte) (*pb.Packet, error) {
	if len(frame) < 4 {
		return nil, ErrFraming
	}
	n := binary.BigEndian.Uint32(frame)
	if n == 0 || n > MaxPacket || uint64(n)+4 != uint64(len(frame)) {
		return nil, ErrFraming
	}
	return decodeBody(frame[4:])
}

func decodeBody(b []byte) (*pb.Packet, error) {
	sid, body := false, false
	for rest := b; len(rest) > 0; {
		num, typ, n := protowire.ConsumeTag(rest)
		if n < 0 {
			return nil, ErrFraming
		}
		rest = rest[n:]
		switch {
		case num == 1 && typ == protowire.VarintType && !sid:
			v, k := protowire.ConsumeVarint(rest)
			if k < 0 || v > math.MaxUint32 {
				return nil, ErrFraming
			}
			rest = rest[k:]
			sid = true
		case num >= 10 && num <= 19 && typ == protowire.BytesType && !body:
			_, k := protowire.ConsumeBytes(rest)
			if k < 0 {
				return nil, ErrFraming
			}
			rest = rest[k:]
			body = true
		default:
			return nil, ErrFraming
		}
	}
	if !body {
		return nil, ErrFraming
	}
	p := new(pb.Packet)
	if err := proto.Unmarshal(b, p); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFraming, err)
	}
	if err := validatePacket(p); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFraming, err)
	}
	return p, nil
}

// Decoder is a terminal-on-error incremental decoder. Its zero value is ready.
// Feed retains at most one packet (MaxPacket+4 bytes) and calls yield before
// reading another frame. Callbacks must not reenter the decoder.
type Decoder struct {
	header   [4]byte
	headerN  int
	body     []byte
	n        int
	err      error
	finished bool
}

func (d *Decoder) Feed(b []byte, yield func(*pb.Packet) error) error {
	if d.err != nil {
		return d.err
	}
	if d.finished {
		return ErrState
	}
	if yield == nil {
		return ErrArgument
	}
	for len(b) > 0 {
		if d.headerN < 4 {
			k := copy(d.header[d.headerN:], b)
			d.headerN += k
			b = b[k:]
			if d.headerN < 4 {
				continue
			}
			n := binary.BigEndian.Uint32(d.header[:])
			if n == 0 || n > MaxPacket {
				d.err = ErrFraming
				return d.err
			}
			d.body = make([]byte, int(n))
		}
		k := copy(d.body[d.n:], b)
		d.n += k
		b = b[k:]
		if d.n < len(d.body) {
			continue
		}
		p, err := decodeBody(d.body)
		d.body = nil
		d.n = 0
		d.headerN = 0
		if err == nil {
			err = yield(p)
		}
		if err != nil {
			d.err = err
			return err
		}
	}
	return nil
}

// Finish validates EOF and forbids further input.
func (d *Decoder) Finish() error {
	if d.err != nil {
		return d.err
	}
	d.finished = true
	if d.headerN != 0 {
		d.err = io.ErrUnexpectedEOF
	}
	return d.err
}

// ReadPacket reads exactly one frame, validating the size before allocating.
func ReadPacket(r io.Reader) (*pb.Packet, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n == 0 || n > MaxPacket {
		return nil, ErrFraming
	}
	b := make([]byte, int(n))
	if _, err := io.ReadFull(r, b); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return decodeBody(b)
}

// WritePacket fully writes a frame, including when w performs short writes.
// A write error is terminal for its Session: never retry a partial frame.
func WritePacket(w io.Writer, p *pb.Packet) error {
	b, err := Encode(p)
	if err != nil {
		return err
	}
	for len(b) > 0 {
		n, err := w.Write(b)
		if n < 0 || n > len(b) {
			return io.ErrShortWrite
		}
		b = b[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
