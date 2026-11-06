package tm_test

import (
	"fmt"

	tm "github.com/lib-x/tm/sdk/go"
	pb "github.com/lib-x/tm/sdk/go/pb/tmp/v1"
)

func ExampleSession() {
	a, _ := tm.NewSession(tm.Initiator, tm.DefaultSettings(), []string{"bytes.v1"})
	b, _ := tm.NewSession(tm.Responder, tm.DefaultSettings(), []string{"bytes.v1"})
	hello, _ := a.Start()
	ready, _ := b.Receive(hello)
	_, _ = a.Receive(ready.Outbound[0])
	id, open, _ := a.Open(&pb.Open{Profile: "bytes.v1", Route: "echo"})
	_, _ = b.Receive(open)
	// The host authorizes this profile/route before accepting.
	ack, _ := b.Accept(id)
	_, _ = a.Receive(ack)
	data, _ := a.Send(id, []byte("hello"), true)
	r, _ := b.Receive(data)
	fmt.Println(string(r.Events[0].Payload), r.Events[0].Fin)
	windows, _ := b.Consume(id, 5)
	for _, p := range windows {
		_, _ = a.Receive(p)
	}
	reply, _ := b.Send(id, []byte("ok"), true)
	r, _ = a.Receive(reply)
	fmt.Println(string(r.Events[0].Payload), r.Events[0].Fin)
	windows, _ = a.Consume(id, 2)
	for _, p := range windows {
		_, _ = b.Receive(p)
	}
	// Output:
	// hello true
	// ok true
}
