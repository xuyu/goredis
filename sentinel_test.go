package goredis

import "testing"

func init() {
	address = "127.0.0.1:6379"
	client, err := Dial(&DialConfig{network, address, db, password, timeout, maxidle})
	if err != nil {
		panic(err)
	}
	r = client
}

func TestBuildSlaveInfo(t *testing.T) {

	inmap := make(map[string]string)
	inmap["name"] = "apod"
	inmap["ip"] = "127.0.0.1"
	inmap["port"] = "6379"
	inmap["runid"] = "thisismyrunid"
	inmap["flags"] = "slaveflag"
	inmap["pending-commands"] = "3"
	inmap["is-master-down"] = "1"
	inmap["last-ok-ping-reply"] = "100"
	inmap["role-reported-time"] = "12345678"
	inmap["last-ping-reply"] = "12345600"
	inmap["last-ping-sent"] = "12345599"
	inmap["role-reported"] = "slave"
	inmap["info-refresh"] = "200"
	inmap["master-link-down-time"] = "10"
	inmap["master-link-status"] = "ok"
	inmap["master-host"] = "127.0.0.2"
	inmap["master-port"] = "6379"
	inmap["slave-priority"] = "100"
	inmap["slave-repl-offset"] = "202"

	slave_info, err := r.buildSlaveInfoStruct(inmap)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	if slave_info.Flags != "slaveflag" {
		t.Fail()
	}
	if slave_info.InfoRefresh != 200 {
		t.Fail()
	}
}

func TestBuildMasterInfo(t *testing.T) {

	inmap := make(map[string]string)
	inmap["name"] = "apod"
	inmap["ip"] = "127.0.0.1"
	inmap["port"] = "6379"
	inmap["runid"] = "thisismyrunid"
	inmap["flags"] = "masterflag"
	inmap["down-after-milliseconds"] = "30000"
	inmap["last-ok-ping-reply"] = "100"
	inmap["role-reported-time"] = "12345678"
	inmap["last-ping-reply"] = "12345600"
	inmap["last-ping-sent"] = "12345599"
	inmap["role-reported"] = "slave"
	inmap["info-refresh"] = "200"
	inmap["config-epoch"] = "10"
	inmap["parallel-syncs"] = "5"
	inmap["failover-timeout"] = "5000"
	inmap["quorum"] = "2"
	inmap["num-slaves"] = "2"
	inmap["num-other-sentinels"] = "2"
	inmap["is-master-down"] = "1"

	info, err := r.buildMasterInfoStruct(inmap)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	if info.Flags != "masterflag" {
		t.Fail()
	}

	if info.Quorum != 2 {
		t.Fail()
	}
	if !info.IsMasterDown {
		t.Fail()
	}

}
