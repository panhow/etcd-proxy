package etcd

import (
	"crypto/tls"
	"crypto/x509"
	"os"
)

type Option func(cli *HTTPClient) error

func WithClientTlsConfig(certFile, keyFile, caCertFile string) Option {
	return func(cli *HTTPClient) (err error) {
		certBytes, err := os.ReadFile(certFile)
		if err != nil {
			return
		}
		keyBytes, err := os.ReadFile(keyFile)
		if err != nil {
			return
		}
		caCertBytes, err := os.ReadFile(caCertFile)
		if err != nil {
			return
		}

		cert, err := tls.X509KeyPair(certBytes, keyBytes)
		if err != nil {
			return
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caCertBytes)

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
		}
		cli.tlsConfig = tlsConfig

		return
	}
}
