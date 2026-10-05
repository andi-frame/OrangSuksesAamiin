module github.com/andi-frame/orangsuksesaamiin/services/consumer-hazard

go 1.25.3

require (
	github.com/andi-frame/orangsuksesaamiin/pkg/obs v0.0.0
	github.com/twmb/franz-go v1.21.7
)

require (
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
)

replace github.com/andi-frame/orangsuksesaamiin/pkg/obs => ../../pkg/obs
