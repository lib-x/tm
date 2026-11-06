package tm

import (
	"errors"
	"testing"

	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
)

func TestGoAwayRejectsPendingBeforeFrame(t *testing.T) {
	a, b := pair(t, DefaultSettings())
	accepted := openStream(t, a, b, true)
	pending := openStream(t, a, b, false)
	ps, err := b.GoAway(pb.ErrorCode_CANCELLED, "drain")
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 2 || ps[0].StreamId != pending || ps[0].GetReset_().Code != pb.ErrorCode_REFUSED || ps[1].GetGoAway().LastStreamId != accepted {
		t.Fatal(ps)
	}
	if b.streams[pending] != nil || !b.draining {
		t.Fatal("pending retained")
	}
	if _, err := b.Accept(pending); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	for _, p := range ps {
		deliver(t, a, p)
	}
	if a.streams[pending] != nil || a.streams[accepted] == nil {
		t.Fatal("wrong streams cancelled")
	}
	if _, _, err := b.Open(&pb.Open{Profile: "bytes.v1"}); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	sendData(t, a, b, accepted, "still open", false)
	ping, err := a.Ping(0xfedcba9876543210)
	if err != nil {
		t.Fatal(err)
	}
	r := deliver(t, b, ping)
	pong := deliver(t, a, r.Outbound[0])
	if pong.Events[0].Kind != ReceivedPong || pong.Events[0].Token != 0xfedcba9876543210 {
		t.Fatal(pong)
	}
}

func TestGoAwayCancelsAllPendingRegardlessCutoff(t *testing.T) {
	a, b := pair(t, DefaultSettings())
	first := openStream(t, a, b, false)
	accepted := openStream(t, a, b, true)
	last := openStream(t, a, b, false)
	remote := openStream(t, b, a, false)
	r := deliver(t, a, goAwayPacket(last))
	if len(r.Events) != 4 || r.Events[0].ID != first || r.Events[1].ID != last || r.Events[2].ID != remote || r.Events[3].Kind != ReceivedGoAway {
		t.Fatal(r)
	}
	for _, event := range r.Events[:3] {
		if event.Kind != StreamReset || event.Code != pb.ErrorCode_REFUSED {
			t.Fatal(event)
		}
	}
	if len(r.Outbound) != 1 || r.Outbound[0].StreamId != remote || r.Outbound[0].GetReset_().Code != pb.ErrorCode_REFUSED {
		t.Fatal(r)
	}
	if len(a.streams) != 1 || a.streams[accepted] == nil {
		t.Fatal("accepted removed")
	}
	if _, err := a.Accept(remote); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	r = deliver(t, a, &pb.Packet{StreamId: remote + 2, Body: &pb.Packet_Open{Open: &pb.Open{Profile: "bytes.v1"}}})
	if len(r.Events) != 0 || r.Outbound[0].GetReset_().Code != pb.ErrorCode_REFUSED || a.highestRemote != remote+2 {
		t.Fatal(r)
	}
}

func TestGoAwayCutoffCannotRevokeHistoricalAccepted(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "closed"}[closed], func(t *testing.T) {
			a, b := pair(t, DefaultSettings())
			id := openStream(t, a, b, true)
			if closed {
				sendData(t, a, b, id, "", true)
				sendData(t, b, a, id, "", true)
				if a.streams[id] != nil {
					t.Fatal("not closed")
				}
			}
			if a.highestAcked != id {
				t.Fatal("missing persistent high water")
			}
			if _, err := a.Receive(goAwayPacket(0)); !errors.Is(err, ErrProtocol) {
				t.Fatal(err)
			}
		})
	}
}

func TestGoAwayPendingLateAcceptBranches(t *testing.T) {
	t.Run("goaway first", func(t *testing.T) {
		a, b := pair(t, DefaultSettings())
		id := openStream(t, a, b, false)
		ps, err := b.GoAway(0, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Accept(id); !errors.Is(err, ErrState) {
			t.Fatal(err)
		}
		for _, p := range ps {
			deliver(t, a, p)
		}
		if a.streams[id] != nil || b.streams[id] != nil {
			t.Fatal("pending survived")
		}
	})
	t.Run("ack first", func(t *testing.T) {
		a, b := pair(t, DefaultSettings())
		id := openStream(t, a, b, false)
		ack, err := b.Accept(id)
		if err != nil {
			t.Fatal(err)
		}
		deliver(t, a, ack)
		ps, err := b.GoAway(0, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(ps) != 1 || ps[0].GetGoAway().LastStreamId != id {
			t.Fatal(ps)
		}
		deliver(t, a, ps[0])
		sendData(t, a, b, id, "accepted survives", false)
	})
	t.Run("peer goaway while open pending", func(t *testing.T) {
		a, b := pair(t, DefaultSettings())
		id := openStream(t, b, a, false)
		r := deliver(t, a, goAwayPacket(0))
		if len(r.Outbound) != 1 || r.Outbound[0].StreamId != id {
			t.Fatal(r)
		}
		if _, err := a.Accept(id); !errors.Is(err, ErrState) {
			t.Fatal(err)
		}
		r = deliver(t, b, r.Outbound[0])
		if r.Events[0].Kind != StreamReset {
			t.Fatal(r)
		}
	})
}

func TestGoAwayCutoffValidationAndRepetition(t *testing.T) {
	for _, last := range []uint32{2, 3} {
		t.Run(string(rune('0'+last)), func(t *testing.T) {
			a, b := pair(t, DefaultSettings())
			openStream(t, a, b, false)
			if _, err := a.Receive(goAwayPacket(last)); !errors.Is(err, ErrProtocol) {
				t.Fatal(err)
			}
		})
	}
	a, b := pair(t, DefaultSettings())
	openStream(t, a, b, false)
	openStream(t, a, b, false)
	deliver(t, a, goAwayPacket(3))
	deliver(t, a, goAwayPacket(1))
	deliver(t, a, goAwayPacket(1))
	if _, err := a.Receive(goAwayPacket(3)); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestPeerSettingsAndNegotiatedProfilesAreCopies(t *testing.T) {
	s := mustSession(t, Initiator, DefaultSettings())
	if s.PeerSettings() != nil {
		t.Fatal("premature settings")
	}
	a, _ := pair(t, DefaultSettings())
	peer := a.PeerSettings()
	peer.MaxData = 1
	profiles := a.NegotiatedProfiles()
	profiles[0] = "changed"
	if a.PeerSettings().MaxData != 16384 || a.NegotiatedProfiles()[0] != "bytes.v1" {
		t.Fatal("getters alias state")
	}
	a.Close()
	if len(a.Close()) != 0 {
		t.Fatal("duplicate close notification")
	}
}
