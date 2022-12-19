package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/panhow/etcd-proxy/util"
	"go.uber.org/zap"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"sync/atomic"
)

type HTTPClient struct {
	tlsConfig   *tls.Config
	count       int64
	endpoints   []string
	cli         *http.Client
	getEndpoint func() string
}

type Result struct {
	Code   int
	Header http.Header
	Body   []byte
	Error  error
}

func NewHTTPClient(endpoints []string, options ...Option) *HTTPClient {
	index := 0
	cli := &HTTPClient{
		count:     0,
		endpoints: endpoints,
		getEndpoint: func() string {
			e := endpoints[index]
			index = (index + 1) % len(endpoints)
			return e
		},
	}

	for _, option := range options {
		err := option(cli)
		if err != nil {
			panic(fmt.Sprintf("init etcd http client failed: %v", err))
		}
	}

	cli.cli = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        0, // no limit
			MaxIdleConnsPerHost: 128,
			MaxConnsPerHost:     0, // no limit
			IdleConnTimeout:     0, // no limit
			TLSClientConfig:     cli.tlsConfig,
		},
		Jar:     &cookiejar.Jar{},
		Timeout: 0,
	}

	return cli
}

func (c *HTTPClient) Do(r *http.Request) (int, http.Header, io.Reader, error) {
	var fail = func(err error) (int, http.Header, io.Reader, error) { return 0, nil, nil, err }
	outR := r.Clone(context.Background())

	if outR.URL.Scheme == "" {
		outR.URL.Scheme = "http"
	}
	outR.URL.Host = c.getEndpoint()
	outR.Close = false

	removeConnectionHeaders(outR.Header)

	for _, h := range hopHeaders {
		hv := outR.Header.Get(h)
		if hv == "" {
			continue
		}
		if h == "Te" && hv == "trailers" {
			continue
		}
		outR.Header.Del(h)
	}

	if _, ok := r.URL.Query()["wait"]; ok {
		atomic.AddInt64(&c.count, 1)
		util.Logger.Info("etcdClient new job", zap.Int64("total", c.count))
		defer func() {
			atomic.AddInt64(&c.count, -1)
			util.Logger.Info("etcdClient done job", zap.Int64("total", c.count))
		}()
	}
	res, err := c.cli.Transport.RoundTrip(outR)
	if err != nil {
		return fail(err)
	}
	defer func() {
		res.Body.Close()
	}()
	removeConnectionHeaders(res.Header)
	for _, h := range hopHeaders {
		res.Header.Del(h)
	}

	statusCode := res.StatusCode
	header := http.Header{}
	util.CopyHeader(header, res.Header)
	if err != nil {
		return fail(err)
	}

	return statusCode, header, res.Body, nil
}

// removeConnectionHeaders removes hop-by-hop headers listed in the "Connection" header of h.
// See RFC 7230, section 6.1
func removeConnectionHeaders(h http.Header) {
	for _, f := range h["Connection"] {
		for _, sf := range strings.Split(f, ",") {
			if sf = strings.TrimSpace(sf); sf != "" {
				h.Del(sf)
			}
		}
	}
}
