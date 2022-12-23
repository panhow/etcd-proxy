package watch

import (
	"bufio"
	"container/list"
	"github.com/panhow/etcd-proxy/etcd"
	"github.com/panhow/etcd-proxy/util"
	"go.uber.org/zap"
	"io"
	"net/http"
	"sync"
)

type WatcherHub struct {
	mutex    sync.Mutex
	watchers map[string]*watchUnit
	cli      *etcd.HTTPClient
}

type watchUnit struct {
	l            *list.List
	headerResult *Result
	bodyCache    []byte
	body         io.ReadCloser
}

func NewWatcherHub(cli *etcd.HTTPClient) *WatcherHub {
	return &WatcherHub{
		mutex:    sync.Mutex{},
		watchers: make(map[string]*watchUnit),
		cli:      cli,
	}
}

func (wh *WatcherHub) GetWatcher(request *http.Request) *Watcher {
	wh.mutex.Lock()
	defer wh.mutex.Unlock()

	key := wh.cli.KeyAdapter(request)
	w := &Watcher{
		key:        key,
		headerChan: make(chan *Result, 1),
		ech:        make(chan []byte, 1),
		hub:        wh,
	}

	unit, ok := wh.watchers[key]

	var elem *list.Element

	if ok { // add the new watcher to the back of the list
		l := unit.l
		elem = l.PushBack(w)
		util.Logger.Info("add watchers", zap.String("key", key), zap.Int("current", l.Len()))
		//if unit.headerResult == nil {
		//	util.Logger.Error("invalid watcher unit, no header result", zap.String("key", key))
		//}
		w.headerChan <- unit.headerResult
		w.notify(unit.bodyCache)
	} else { // create a new list and add the new watcher
		unit = &watchUnit{
			l:         list.New(),
			bodyCache: make([]byte, 0, 1024),
		}

		l := unit.l
		elem = l.PushBack(w)
		util.Logger.Info("new watcher list", zap.String("key", w.key))
		util.Logger.Info("etcdClient new job", zap.String("key", w.key))
		code, header, bodyReader, err := wh.cli.Do(request)
		unit.body = bodyReader
		unit.headerResult = &Result{
			Code:   code,
			Header: header,
			Error:  err,
		}
		if err == nil {
			wh.watchers[key] = unit
			go func() {
				w.headerChan <- unit.headerResult
				r := bufio.NewReader(bodyReader)
				for {
					line, err := r.ReadBytes('\n')
					if err == nil || err == io.EOF {
						if len(line) != 0 {
							wh.notify(key, line)
						}
						if err == io.EOF {
							break
						}
					} else {
						// todo: handle error
						break
					}
				}
			}()
		} else {
			util.Logger.Error("call etcd endpoint failed", zap.Error(err))
		}
	}

	w.remove = func() {
		l := unit.l

		if w.removed { // avoid removing it twice
			return
		}
		w.removed = true
		l.Remove(elem)
		util.Logger.Info(
			"remove watcher",
			zap.String("key", w.key),
			zap.Int("rest", l.Len()),
		)
		if l.Len() == 0 {
			util.Logger.Info(
				"all watchers removed",
				zap.String("key", w.key),
			)
			_ = unit.body.Close()
			util.Logger.Info("etcdClient done job", zap.String("key", key))
			delete(wh.watchers, w.key)
		}
	}

	return w
}

func (wh *WatcherHub) notify(key string, line []byte) {
	wh.mutex.Lock()
	defer wh.mutex.Unlock()

	unit, ok := wh.watchers[key]
	if ok && unit.l != nil {
		unit.bodyCache = append(unit.bodyCache, line...)

		l := unit.l
		curr := l.Front()

		for curr != nil {
			next := curr.Next() // save reference to the next one in the list

			w, _ := curr.Value.(*Watcher)
			w.notify(line)

			curr = next // update current to the next element in the list
		}

	}
}
