package extproc

import (
	"context"
	"sync"
	"sync/atomic"
)

type shutdownPhase struct {
	once sync.Once
	err  error
}

func (p *shutdownPhase) run(shutdown func() error) error {
	p.once.Do(func() {
		p.err = shutdown()
	})
	return p.err
}

type serverLifecycle struct {
	stopping  atomic.Bool
	serving   shutdownPhase
	resources shutdownPhase

	watchMu     sync.Mutex
	watchCancel context.CancelFunc
	watchDone   chan struct{}
}

func (l *serverLifecycle) startWatcher(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	l.watchMu.Lock()
	l.watchCancel = cancel
	l.watchDone = done
	if l.stopping.Load() {
		cancel()
	}
	l.watchMu.Unlock()

	return ctx, func() {
		cancel()
		close(done)
		l.watchMu.Lock()
		if l.watchDone == done {
			l.watchCancel = nil
		}
		l.watchMu.Unlock()
	}
}

func (l *serverLifecycle) beginShutdown() {
	l.stopping.Store(true)
	l.watchMu.Lock()
	if l.watchCancel != nil {
		l.watchCancel()
	}
	l.watchMu.Unlock()
}

func (l *serverLifecycle) waitForWatcher(ctx context.Context) error {
	l.watchMu.Lock()
	done := l.watchDone
	l.watchMu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		select {
		case <-done:
			return nil
		default:
			return ctx.Err()
		}
	}
}

func (l *serverLifecycle) isStopping() bool {
	return l.stopping.Load()
}
