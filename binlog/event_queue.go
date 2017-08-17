package binlog

import (
	"context"
)

type EventQueue struct {
	ch    chan Event
	errCh chan error
	err   error
}

func (q *EventQueue) Pop(ctx context.Context) (Event, error) {
	if q.err != nil {
		return nil, q.err
	}

	select {
	case event := <-q.ch:
		return event, nil
	case q.err = <-q.errCh:
		return nil, q.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
