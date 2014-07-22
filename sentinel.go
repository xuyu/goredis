package goredis

import (
	"fmt"
	"reflect"
	"strconv"
)

// MasterAddress is a small struct to provide connection information for a
// Master as returned from get-master-addr-by-name
type MasterAddress struct {
	Host string
	Port int
}

// MasterInfo is a struct providing the information available from sentinel
// about a given master (aka pod)
// The way this works is you tag the field with the name redis returns
// and reflect is used in the methods which return this structure to populate
// it with the data from Redis
//
// Note this means it will nee dto be updated when new fields are added in
// sentinel. Fortunately this appears to be rare.
//
// Currently the list is:
// 'pending-commands'
// 'ip'
// 'down-after-milliseconds'
// 'role-reported'
// 'runid'
// 'port'
// 'last-ok-ping-reply'
// 'last-ping-sent'
// 'failover-timeout'
// 'config-epoch'
// 'quorum'
// 'role-reported-time'
// 'last-ping-reply'
// 'name'
// 'parallel-syncs'
// 'info-refresh'
// 'flags'
// 'num-slaves'
// 'num-other-sentinels'
type MasterInfo struct {
	Name                  string `redis:"name"`
	Port                  int    `redis:"port"`
	NumSlaves             int    `redis:"num-slaves"`
	Quorum                int    `redis:"quorum"`
	NumOtherSentinels     int    `redis:"num-other-sentinels"`
	ParallelSyncs         int    `redis:"parallel-syncs"`
	RoleReported          string `redis:"role-reported"`
	Runid                 string `redis:"runid"`
	IP                    string `redis:"ip"`
	DownAfterMilliseconds int    `redis:"down-after-milliseconds"`
	IsMasterDown          bool   `redis:"is-master-down"`
	LastOkPingReply       int    `redis:"last-ok-ping-reply"`
	LastPingSent          int    `redis:"last-ping-sent"`
	FailoverTimeout       int    `redis:"failover-timeout"`
	ConfigEpoch           int    `redis:"config-epoch"`
	RoleReportedTime      int    `redis:"role-reported-time"`
	LastPingReply         int    `redis:"last-ping-reply"`
	InfoRefresh           int    `redis:"info-refresh"`
	Flags                 string `redis:"flags"`
}

// SentinelMonitor executes the SENTINEL MONITOR command on the server
// This is used to add pods to the sentinel configuration
func (r *Redis) SentinelMonitor(podname string, ip string, port int, quorum int) error {
	_, err := r.ExecuteCommand("SENTINEL", "MONITOR", podname, ip, port, quorum)
	return err
}

// SentinelMonitor will set the value to be used in the AUTH command for a
// given pod
func (r *Redis) SentinelSetPass(podname string, password string) error {
	_, err := r.ExecuteCommand("SENTINEL", "SET", podname, "AUTHPASS", password)
	return err
}

// SentinelMasterInfo returns the information about a pod or master
func (r *Redis) SentinelMasterInfo(podname string) (master MasterInfo, err error) {
	rp, err := r.ExecuteCommand("SENTINEL", "MASTER", podname)
	if err != nil {
		return master, err
	}
	info, err := rp.HashValue()
	s := reflect.ValueOf(&master).Elem()
	typeOfT := s.Type()
	for i := 0; i < s.NumField(); i++ {
		p := typeOfT.Field(i)
		f := s.Field(i)
		tag := p.Tag.Get("redis")
		if f.Type().Name() == "int" {
			val, err := strconv.ParseInt(info[tag], 10, 64)
			if err != nil {
				println("Unable to convert to data from sentinel server:", info[tag])
			} else {
				f.SetInt(val)
			}
		}
		if f.Type().Name() == "string" {
			f.SetString(info[tag])
		}
		if f.Type().Name() == "bool" {
			// This handles primarily the xxx_xx style fields in the return data from redis
			if info[tag] != "" {
				println(tag, ":=", info[tag])
				val, err := strconv.ParseInt(info[tag], 10, 64)
				if err != nil {
					println("Unable to convert to data from sentinel server:", info[tag])
					fmt.Println("Error:", err)
				} else {
					if val > 0 {
						f.SetBool(true)
					}
				}
			}
		}
	}
	return
}

// SentinelGetMaster returns the information needed to connect to the master of
// a given pod
func (r *Redis) SentinelGetMaster(podname string) (conninfo MasterAddress, err error) {
	rp, err := r.ExecuteCommand("SENTINEL", "get-master-addr-by-name", podname)
	if err != nil {
		return conninfo, err
	}
	info, err := rp.ListValue()
	conninfo.Host = info[0]
	conninfo.Port, err = strconv.Atoi(info[1])
	if err != nil {
		fmt.Println("Got bad port info from server, causing err:", err)
	}
	return conninfo, err
}
