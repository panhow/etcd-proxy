package watch

import (
	"github.com/panhow/etcd-proxy/etcd"
	"github.com/panhow/etcd-proxy/util"
	"go.uber.org/zap"
)

type Watcher struct {
	key     string
	ech     chan etcd.Result
	hub     *WatcherHub
	removed bool
	remove  func()
}

func (w *Watcher) Key() string {
	return w.key
}

func (w *Watcher) EventChannel() <-chan etcd.Result {
	return w.ech
}

func (w *Watcher) Remove() {
	w.hub.mutex.Lock()
	defer w.hub.mutex.Unlock()

	close(w.ech)
	if w.remove != nil {
		w.remove()
	}
}

func (w *Watcher) notify(r etcd.Result) {
	util.Logger.Info("prepare push message into channel", zap.String("key", w.key))
	select {
	case w.ech <- r:
		util.Logger.Info("done push message into channel", zap.String("key", w.key))
	default:
		// We have missed a notification. Remove the watcher.
		// Removing the watcher also closes the eventChan.
		w.remove()
	}
}
