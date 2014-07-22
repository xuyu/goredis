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
type MasterInfo struct {
	Name      string `redis:"name"`
	Port      int    `redis:"port"`
	NumSlaves int    `redis:"num-slaves"`
	Quorum    int    `redis:"quorum"`
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
