package goredis

import (
	"fmt"
	"log"
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
	Runid                 string `redis:"runid"`
	IP                    string `redis:"ip"`
	DownAfterMilliseconds int    `redis:"down-after-milliseconds"`
	IsMasterDown          bool   `redis:"is-master-down"`
	LastOkPingReply       int    `redis:"last-ok-ping-reply"`
	RoleReportedTime      int    `redis:"role-reported-time"`
	InfoRefresh           int    `redis:"info-refresh"`
	RoleReported          string `redis:"role-reported"`
	LastPingReply         int    `redis:"last-ping-reply"`
	LastPingSent          int    `redis:"last-ping-sent"`
	FailoverTimeout       int    `redis:"failover-timeout"`
	ConfigEpoch           int    `redis:"config-epoch"`
	Flags                 string `redis:"flags"`
}

// SlaveInfo is a struct for the results returned from slave queries,
// specifically the individual entries of the  `sentinel slave <podname>`
// command. As with the other Sentinel structs this may change and will need
// updated for new entries
// Currently the members defined by sentinel are as follows.
// "name"
// "ip"
// "port"
// "runid"
// "flags"
// "pending-commands"
// "last-ping-sent"
// "last-ok-ping-reply"
// "last-ping-reply"
// "down-after-milliseconds"
// "info-refresh"
// "role-reported"
// "role-reported-time"
// "master-link-down-time"
// "master-link-status"
// "master-host"
// "master-port"
// "slave-priority"
// "slave-repl-offset"
type SlaveInfo struct {
	Name                   string `redis:"name"`
	Host                   string `redis:"ip"`
	Port                   int    `redis:"port"`
	Runid                  string `redis:"runid"`
	Flags                  string `redis:"flags"`
	PendingCommands        int    `redis:"pending-commands"`
	IsMasterDown           bool   `redis:"is-master-down"`
	LastOkPingReply        int    `redis:"last-ok-ping-reply"`
	RoleReportedTime       int    `redis:"role-reported-time"`
	LastPingReply          int    `redis:"last-ping-reply"`
	LastPingSent           int    `redis:"last-ping-sent"`
	InfoRefresh            int    `redis:"info-refresh"`
	RoleReported           string `redis:"role-reported"`
	MasterLinkDownTime     int    `redis:"master-link-down-time"`
	MasterLinkStatus       string `redis:"master-link-status"`
	MasterHost             string `redis:"master-host"`
	MasterPort             int    `redis:"master-port"`
	SlavePriority          int    `redis:"slave-priority"`
	SlaveReplicationOffset int    `redis:"slave-repl-offset"`
}

// buildSlaveInfoStruct builods the struct for a slave from the Redis slaves command
func buildSlaveInfoStruct(info map[string]string) (master SlaveInfo, err error) {
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

// SentinelSlaves takes a podname and returns a list of SlaveInfo structs for
// each known slave.
func (r *Redis) SentinelSlaves(podname string) (slaves []SlaveInfo) {
	rp, err := r.ExecuteCommand("SENTINEL", "SLAVES", podname)
	if err != nil {
		fmt.Println("error on slaves command:", err)
		return
	}
	for i := 0; i < len(rp.Multi); i++ {
		slavemap, err := rp.Multi[i].HashValue()
		if err != nil {
			log.Println("unable to get slave info, err:", err)
		} else {
			info, err := buildSlaveInfoStruct(slavemap)
			if err != nil {
				fmt.Printf("Unable to get slaves, err:", err, "\n")
			}
			slaves = append(slaves, info)
		}
	}
	return
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

func (r *Redis) SentinelMasters() (masters []MasterInfo, err error) {
	rp, err := r.ExecuteCommand("SENTINEL", "MASTERS")
	if err != nil {
		return
	}
	podcount := len(rp.Multi)
	println("Found", podcount, "Pods")
	for i := 0; i < podcount; i++ {
		pod, err := rp.Multi[i].HashValue()
		if err != nil {
			log.Fatal("Error:", err)
		}
		minfo, err := buildMasterInfoStruct(pod)
		masters = append(masters, minfo)
	}
	return
}

func buildMasterInfoStruct(info map[string]string) (master MasterInfo, err error) {
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

// SentinelMasterInfo returns the information about a pod or master
func (r *Redis) SentinelMasterInfo(podname string) (master MasterInfo, err error) {
	rp, err := r.ExecuteCommand("SENTINEL", "MASTER", podname)
	if err != nil {
		return master, err
	}
	info, err := rp.HashValue()
	return buildMasterInfoStruct(info)
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
