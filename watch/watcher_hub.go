package watch

import (
	"container/list"
	"github.com/panhow/etcd-proxy/etcd"
	"github.com/panhow/etcd-proxy/util"
	"go.uber.org/zap"
	"net/http"
	"sync"
)

type WatcherHub struct {
	mutex    sync.Mutex
	watchers map[string]*list.List
	cli      *etcd.HTTPClient
}

func NewWatcherHub(cli *etcd.HTTPClient) *WatcherHub {
	return &WatcherHub{
		mutex:    sync.Mutex{},
		watchers: make(map[string]*list.List),
		cli:      cli,
	}
}

func (wh *WatcherHub) GetWatcher(key string, request *http.Request) *Watcher {
	wh.mutex.Lock()
	defer wh.mutex.Unlock()

	w := &Watcher{
		key: key,
		ech: make(chan etcd.Result, 1),
		hub: wh,
	}

	l, ok := wh.watchers[key]

	var elem *list.Element

	if ok { // add the new watcher to the back of the list
		elem = l.PushBack(w)
		util.Logger.Debug("add watchers", zap.String("key", key), zap.Int("current", l.Len()))
	} else { // create a new list and add the new watcher
		l = list.New()
		elem = l.PushBack(w)
		wh.watchers[key] = l
		util.Logger.Info("new watcher list", zap.String("key", w.key))
		go func() {
			util.Logger.Info("new watch task", zap.String("key", w.key))
			code, header, body, err := wh.cli.Do(request)
			result := etcd.Result{
				Code:   code,
				Header: header,
				Body:   body,
				Error:  err,
			}
			wh.notify(key, result)
		}()
	}

	w.remove = func() {
		if w.removed { // avoid removing it twice
			return
		}
		w.removed = true
		l.Remove(elem)
		util.Logger.Debug(
			"remove watcher",
			zap.String("key", w.key),
			zap.Int("rest", l.Len()),
		)
		if l.Len() == 0 {
			util.Logger.Info(
				"all watchers removed",
				zap.String("key", w.key),
			)
		}
	}

	return w
}

func (wh *WatcherHub) notify(key string, r etcd.Result) {
	wh.mutex.Lock()
	defer wh.mutex.Unlock()

	l, ok := wh.watchers[key]
	if ok {
		curr := l.Front()

		for curr != nil {
			next := curr.Next() // save reference to the next one in the list

			w, _ := curr.Value.(*Watcher)
			w.notify(r)
			w.removed = true
			l.Remove(curr)

			curr = next // update current to the next element in the list
		}

		if l.Len() == 0 {
			delete(wh.watchers, key)
		}
	}
}
