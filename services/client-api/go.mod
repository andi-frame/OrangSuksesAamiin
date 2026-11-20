module github.com/andi-frame/orangsuksesaamiin/services/client-api

go 1.25.3

require (
	github.com/andi-frame/orangsuksesaamiin/pkg/obs v0.0.0
	github.com/golang-jwt/jwt/v5 v5.2.2
	golang.org/x/time v0.11.0
)

require (
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go v1.21.7
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
)

replace github.com/andi-frame/orangsuksesaamiin/pkg/obs => ../../pkg/obs
