package watch

import (
	"github.com/panhow/etcd-proxy/util"
	"go.uber.org/zap"
)

type Watcher struct {
	key        string
	headerChan chan *Result
	ech        chan []byte
	hub        *WatcherHub
	removed    bool
	remove     func()
}

func (w *Watcher) Key() string {
	return w.key
}

func (w *Watcher) EventChannel() <-chan []byte {
	return w.ech
}

func (w *Watcher) Header() *Result {
	result := <-w.headerChan

	close(w.headerChan)
	w.headerChan = nil

	return result
}

func (w *Watcher) Remove() {
	w.hub.mutex.Lock()
	defer w.hub.mutex.Unlock()

	close(w.ech)
	if w.remove != nil {
		w.remove()
	}
}

func (w *Watcher) notify(r []byte) {
	util.Logger.Debug("prepare push message into channel", zap.String("key", w.key))
	select {
	case w.ech <- r:
		util.Logger.Debug("done push message into channel", zap.String("key", w.key))
	default:
		// We have missed a notification. Remove the watcher.
		// Removing the watcher also closes the eventChan.
		w.remove()
	}
}
