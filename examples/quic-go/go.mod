module github.com/lib-x/tm/examples/quic-go

go 1.26.0

require (
	github.com/lib-x/tm/sdk/go v0.1.0
	github.com/quic-go/quic-go v0.65.0
)

require (
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/lib-x/tm/sdk/go => ../../sdk/go
