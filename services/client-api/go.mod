module github.com/andi-frame/orangsuksesaamiin/services/client-api

go 1.25.3

require (
	github.com/andi-frame/orangsuksesaamiin/pkg/obs v0.0.0
	github.com/golang-jwt/jwt/v5 v5.2.2
	golang.org/x/time v0.11.0
)

replace github.com/andi-frame/orangsuksesaamiin/pkg/obs => ../../pkg/obs
