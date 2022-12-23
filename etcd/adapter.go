package etcd

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/panhow/etcd-proxy/util"
	"go.uber.org/zap"
	"io"
	"net/http"
	"time"
)

type KeyAdapter func(r *http.Request) string

func V2Adapter(r *http.Request) string {
	return r.URL.String()
}

func V3Adapter(r *http.Request) string {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		util.Logger.Error("read body from client failed", zap.Error(err))
		return time.Now().String()
	}

	req := &watchReq{}
	err = json.Unmarshal(bodyBytes, req)
	if err != nil {
		panic(fmt.Sprintf("unmarhsal watch request failed: %+v", err))
	}
	keyDecoded, err := base64.URLEncoding.DecodeString(req.CreateRequest.Key)
	if err != nil {
		panic(fmt.Sprintf("key base64 decode failed: %+v", err))
	}
	key := fmt.Sprintf("%s-%d", keyDecoded, req.CreateRequest.StartRevision)

	//// compute the sha1sum of body
	//h := sha1.New()
	//h.Write(bodyBytes)
	//key := string(h.Sum(nil))

	// set header to request
	reader := io.NopCloser(bytes.NewReader(bodyBytes))
	r.Body = reader
	return key
}

type watchReq struct {
	CreateRequest struct {
		Key           string `json:"key"`
		StartRevision int    `json:"start_revision"`
		RangeEnd      string `json:"range_end"`
	} `json:"create_request"`
}
