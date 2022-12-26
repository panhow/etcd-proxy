package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"github.com/panhow/etcd-proxy/etcd"
	"github.com/panhow/etcd-proxy/util"
	"github.com/panhow/etcd-proxy/watch"
	"go.uber.org/zap"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

var (
	etcdHTTPClient *etcd.HTTPClient
	watcherHub     *watch.WatcherHub
)

var (
	listen    = flag.String("listen", ":5678", "etcd proxy listen addrs")
	endpoints = flag.String("endpoints", "http://127.0.0.1:2379", "etcd endpoints be proxied")
	etcdApiV3 = flag.Bool("etcdv3", false, "default false(means api v2)")

	serverTlsEnable = flag.Bool("serverTlsEnable", false, "default disable by default")
	clientTlsEnable = flag.Bool("clientTlsEnable", false, "default disabled by default,"+
		"if enabled, should use with `cacert`,`cert`,`key` flag")
	caCert = flag.String("cacert", "", "")
	cert   = flag.String("cert", "", "")
	key    = flag.String("key", "", "")
)

func init() {
	flag.Parse()
}

func proxy(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()

	w := watcherHub.GetWatcher(request)

	defer w.Remove()
	ech := w.EventChannel()

	// handle response headers
	headerResponse := w.Header()
	util.CopyHeader(writer.Header(), headerResponse.Header)
	writer.WriteHeader(headerResponse.Code)

	if err := headerResponse.Error; err != nil {
		_, err := writer.Write([]byte(err.Error()))
		if err != nil {
			util.Logger.Error("write error failed", zap.Error(err))
		}
		return
	}

HandleStreamingResponse:
	for {
		select {
		case <-ctx.Done():
			// Timed out. net/http will close the connection for us, so nothing to do.
			util.Logger.Debug(
				"timed out",
				zap.String("remote_addr", strings.Split(request.RemoteAddr, ":")[0]),
				zap.String("key", w.Key()),
			)
			return
		case line, ok := <-ech:
			util.Logger.Debug("receive line from channel", zap.String("key", w.Key()))

			_, err := writer.Write(line)
			if err != nil {
				util.Logger.Error("write response line failed", zap.Error(err))
				break HandleStreamingResponse
			}
			writer.(http.Flusher).Flush()

			if !ok {
				break HandleStreamingResponse
			}
		}
	}
	util.Logger.Info(
		"response",
		zap.String("remote_addr", strings.Split(request.RemoteAddr, ":")[0]),
		zap.Int("code", headerResponse.Code),
	)
}

func direct(writer http.ResponseWriter, request *http.Request, retry int) {
	//fmt.Println(request.RemoteAddr, "direct:", request.Method, request.URL)
	var (
		code   int
		header http.Header
		body   io.ReadCloser
		err    error
	)
	for i := 0; i < 2*retry; i++ {
		code, header, body, err = etcdHTTPClient.Do(request)
		if err == nil {
			break
		}
	}
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = writer.Write([]byte(err.Error()))
		return
	}

	util.CopyHeader(writer.Header(), header)
	writer.WriteHeader(code)
	_, err = io.Copy(writer, body)
	if err != nil {
		util.Logger.Error("write response failed", zap.Error(err))
	}
	_ = body.Close()
}

func main() {
	util.Logger.Info("serve start", zap.Int16("port", 5678), zap.Strings("endpoints", strings.Split(*endpoints, ",")))

	options := make([]etcd.Option, 0, 0)
	if *clientTlsEnable {
		options = append(options, etcd.WithClientTlsConfig(*cert, *key, *caCert))
	}
	if *etcdApiV3 {
		options = append(options, etcd.WithEtcdV3Api())
	}

	etcdHTTPClient = etcd.NewHTTPClient(strings.Split(*endpoints, ","), options...)
	watcherHub = watch.NewWatcherHub(etcdHTTPClient)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		method := r.Method
		_, wait := r.URL.Query()["wait"]
		if wait && method == http.MethodGet || r.URL.Path == "/v3/watch" {
			proxy(w, r)
			//direct(w, r)
		} else {
			direct(w, r, strings.Count(*endpoints, ",")+1)
		}
	})
	server := http.Server{
		Addr:      *listen,
		TLSConfig: buildTlsConfigFromFlag(),
	}
	var err error
	if *serverTlsEnable {
		err = server.ListenAndServeTLS(*cert, *key)
	} else {
		err = server.ListenAndServe()
	}
	log.Fatal(err)
}

func buildTlsConfigFromFlag() *tls.Config {
	caCertBytes, err := os.ReadFile(*caCert)
	if err != nil {
		panic(fmt.Sprintf("parse ca failed, err: %+v", err))
	}

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCertBytes)

	return &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
	}
}
