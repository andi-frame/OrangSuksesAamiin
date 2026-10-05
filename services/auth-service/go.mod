module github.com/andi-frame/orangsuksesaamiin/services/auth-service

go 1.25.3

require (
	github.com/andi-frame/orangsuksesaamiin/pkg/obs v0.0.0
	github.com/golang-jwt/jwt/v5 v5.2.2
	modernc.org/sqlite v1.34.5
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/sys v0.22.0 // indirect
	modernc.org/libc v1.55.3 // indirect
	modernc.org/mathutil v1.6.0 // indirect
	modernc.org/memory v1.8.0 // indirect
)

replace github.com/andi-frame/orangsuksesaamiin/pkg/obs => ../../pkg/obs
