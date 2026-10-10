package service

import (
	"context"
	"testing"
)

func TestWaitersReleaseAndWake(t *testing.T) {
	n := NewNotifier(nil)
	a := n.WaitCh("t", "a")
	b := n.WaitCh("t", "a")
	n.ReleaseWait("t", "a", a)
	if len(n.waiters) != 1 {
		t.Fatal("released shared waiter")
	}
	n.changed(context.Background(), "t", "a")
	select {
	case <-b:
	default:
		t.Fatal("did not wake waiter")
	}
	c := n.WaitCh("t", "a")
	n.ReleaseWait("t", "a", b)
	if len(n.waiters) != 1 {
		t.Fatal("old release deleted new waiter")
	}
	n.ReleaseWait("t", "a", c)
	if len(n.waiters) != 0 {
		t.Fatal("empty waiter leaked")
	}
}
