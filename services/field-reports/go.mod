module github.com/andi-frame/orangsuksesaamiin/services/field-reports

go 1.25.3

require (
	github.com/andi-frame/orangsuksesaamiin/pkg/obs v0.0.0
	github.com/twmb/franz-go v1.21.7
	github.com/twmb/franz-go/pkg/kadm v1.18.0
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260916055000-210e42eab3e4
)

require (
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.30 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.14.0 // indirect
	golang.org/x/crypto v0.51.0 // indirect
)

replace github.com/andi-frame/orangsuksesaamiin/pkg/obs => ../../pkg/obs
