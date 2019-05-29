package influx2http

import (
	"net/http"
	"net/url"
)

func NewURL(addr, path string) (*url.URL, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	u.Path = path
	return u, nil
}

func NewClient(scheme string, insecure bool) *http.Client {
	hc := &http.Client{
		Transport: defaultTransport,
	}
	if scheme == "https" && insecure {
		hc.Transport = skipVerifyTransport
	}

	return hc
}
