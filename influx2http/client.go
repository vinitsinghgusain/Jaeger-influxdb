package influx2http

import (
	"net/http"
	"net/url"
)

func newURL(addr, path string) (*url.URL, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	u.Path = path
	return u, nil
}

func newClient(scheme string, insecure bool) *http.Client {
	hc := &http.Client{
		Transport: defaultTransport,
	}
	if scheme == "https" && insecure {
		hc.Transport = skipVerifyTransport
	}

	return hc
}
