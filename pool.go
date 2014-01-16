package goredis

import (
	"container/list"
	"sync"
)

type ConnPool struct {
	MaxIdle int
	Dial    func() (*Connection, error)
	idle    *list.List
	active  int
	mutex   sync.Mutex
}

func NewConnPool(maxidle int, dial func() (*Connection, error)) *ConnPool {
	return &ConnPool{
		MaxIdle: maxidle,
		Dial:    dial,
		idle:    list.New(),
	}
}

func (p *ConnPool) Get() (*Connection, error) {
	p.mutex.Lock()
	p.active++
	if p.idle.Len() > 0 {
		back := p.idle.Back()
		p.idle.Remove(back)
		p.mutex.Unlock()
		return back.Value.(*Connection), nil
	}
	p.mutex.Unlock()
	return p.Dial()
}

func (p *ConnPool) Put(c *Connection) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.active--
	if c == nil {
		return
	}
	if p.idle.Len() >= p.MaxIdle {
		p.idle.Remove(p.idle.Front())
	}
	p.idle.PushBack(c)
}
