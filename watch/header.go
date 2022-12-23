package watch

import "net/http"

type Result struct {
	Code   int
	Header http.Header
	Error  error
}
