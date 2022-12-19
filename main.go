package main

import (
	"bytes"
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

func proxy(writer http.ResponseWriter, request *http.Request) {
	//fmt.Println(request.RemoteAddr, "watch:", request.Method, request.URL)

	key := request.URL.String()
	w := watcherHub.GetWatcher(key, request)

	defer w.Remove()
	ech := w.EventChannel()

	ctx := request.Context()
	for {
		select {
		case <-ctx.Done():
			// Timed out. net/http will close the connection for us, so nothing to do.
			util.Logger.Debug(
				"timed out",
				zap.String("remote_addr", strings.Split(request.RemoteAddr, ":")[0]),
			)
			return
		case result, closed := <-ech:
			util.Logger.Info("receive result from channel", zap.String("key", w.Key()))
			code := 200
			if result.Error != nil {
				code = 502
				writer.WriteHeader(http.StatusInternalServerError)
				_, _ = writer.Write([]byte(result.Error.Error()))
			} else if result.Code != 0 {
				util.CopyHeader(writer.Header(), result.Header)
				writer.WriteHeader(result.Code)
				bytesReader := bytes.NewReader(result.Body)
				_, _ = io.Copy(writer, bytesReader)
			}
			util.Logger.Info(
				"response",
				zap.String("remote_addr", strings.Split(request.RemoteAddr, ":")[0]),
				zap.Int("code", code),
			)
			if closed {
				return
			}
		}
	}

}

func direct(writer http.ResponseWriter, request *http.Request, retry int) {
	//fmt.Println(request.RemoteAddr, "direct:", request.Method, request.URL)
	var (
		code   int
		header http.Header
		body   io.Reader
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
	_, _ = io.Copy(writer, body)
}

func main() {
	endpoints, ok := os.LookupEnv("ETCD_ENDPOINTS")
	strings.TrimSpace(endpoints)
	if !ok || endpoints == "" {
		endpoints = "127.0.0.1:2379"
	}

	util.Logger.Info("serve start", zap.Int16("port", 5678), zap.Strings("endpoints", strings.Split(endpoints, ",")))
	etcdHTTPClient = etcd.NewHTTPClient(strings.Split(endpoints, ","))
	watcherHub = watch.NewWatcherHub(etcdHTTPClient)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		method := r.Method
		_, wait := r.URL.Query()["wait"]
		if wait && method == http.MethodGet {
			proxy(w, r)
			//direct(w, r)
		} else {
			direct(w, r, len(endpoints))
		}
	})
	log.Fatal(http.ListenAndServe(":5678", nil))
}
