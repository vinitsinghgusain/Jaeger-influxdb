package influx2http

import (
	"fmt"
	"net/http"
)

const tokenScheme = "Token " // TODO(goller): I'd like this to be Bearer

// SetToken adds the token to the request.
func SetToken(token string, req *http.Request) {
	req.Header.Set("Authorization", fmt.Sprintf("%s%s", tokenScheme, token))
}
