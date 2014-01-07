package goredis

import (
	"testing"
)

func TestPublish(t *testing.T) {
	if _, err := r.Publish("key", "value"); err != nil {
		t.Error(err)
	}
}

func TestSubscribe(t *testing.T) {
	quit := make(chan bool)
	sub, err := r.PubSub()
	if err != nil {
		t.Error(err)
	}
	defer sub.Close()
	go func() {
		if err := sub.Subscribe("channel"); err != nil {
			t.Error(err)
			quit <- true
		}
		for {
			list, err := sub.Recv()
			if err != nil {
				t.Error(err)
				quit <- true
				break
			}
			if list[0] == "message" {
				if list[1] != "channel" || list[2] != "message" {
					t.Fail()
				}
				quit <- true
			}
		}
	}()
	r.Publish("channel", "message")
	<-quit
}

func TestPSubscribe(t *testing.T) {
	quit := make(chan bool)
	psub, err := r.PubSub()
	if err != nil {
		t.Error(err)
	}
	defer psub.Close()
	go func() {
		if err := psub.PSubscribe("news.*"); err != nil {
			t.Error(err)
			quit <- true
		}
		for {
			list, err := psub.Recv()
			if err != nil {
				t.Error(err)
				quit <- true
				break
			}
			if list[0] == "pmessage" {
				if list[1] != "news.*" || list[2] != "news.china" || list[3] != "message" {
					t.Fail()
				}
				quit <- true
			}
		}
	}()
	r.Publish("news.china", "message")
	<-quit
}

func TestUnSubscribe(t *testing.T) {
	ch := make(chan bool)
	sub, err := r.PubSub()
	if err != nil {
		t.Error(err)
	}
	defer sub.Close()
	go func() {
		for {
			sub.Recv()
			ch <- true
		}
	}()
	sub.Subscribe("channel")
	<-ch
	if len(sub.Channels) != 1 {
		t.Fail()
	}
	if err := sub.UnSubscribe("channel"); err != nil {
		t.Error(err)
	}
	<-ch
	if len(sub.Channels) != 0 {
		t.Fail()
	}
}

func TestPUnSubscribe(t *testing.T) {
	ch := make(chan bool)
	sub, err := r.PubSub()
	if err != nil {
		t.Error(err)
	}
	defer sub.Close()
	go func() {
		for {
			sub.Recv()
			ch <- true
		}
	}()
	sub.PSubscribe("channel.*")
	<-ch
	if len(sub.Patterns) != 1 {
		t.Fail()
	}
	if err := sub.PUnSubscribe("channel.*"); err != nil {
		t.Error(err)
	}
	<-ch
	if len(sub.Patterns) != 0 {
		t.Fail()
	}
}
