# Sentinel Commands

## Overview and Purpose

The SENTINEL commands provide an interface to get information regarding
a Redis replicaiton configuration, aka a 'pod'. It also provides a means
to add or remove pods from a Sentinel's list of monitored pods, to get
or set Sentinel configuration variables for specific pods, and for
obtaining information about the pods under management by the Sentinel
connected to.

# Commands Exported

## SentinelSlaves

### What It Does
This method takes a string argument specifying a podname (aka. a
master) to get a list of slaves for. It will return a SlaveInfo struct
for each slave in the pod.

### Why You Use it

One of the reasons you would want to use this command is to get a list
of slaves for the pod in order to check on their status and monitor
them, or even to use them. It would also let you do some checking of the
pod to ensure it is possible to failover prior to a failed attempt. 

One way to do that is to check the SlavePriority of each slave and raise
an alert if they are all set to '0' - which tells Sentinel to not
failover to that slave.

Another use case is by getting a list of slaves the client code could choose to
execute reads against the slaves instead of the master, thus performing
crude load balancing.

## SentinelMasters

### What It Does

This method provides a list of masters currently managed by the
Sentinel instance, with detailed information about them. 

### WHy You Use It

There are two primary purposes for using this commands: monitoring and
service discovery.

If you name each pod/master based on what it does, for example
"sessionstore", "objectcache", "trafficstats", your client code can
connect to the Sentinel and get a list of masters, select the one it
needs access to and proceed to talk directly to it. If this is all that
is needed the command `SentinelGetMaster` will be more effective.
However with this command you could make decisions on connecticity based
on pod status such as "don't connect if there is no failover".

For monitoring purposes you can determine the quorum status, avialble
slaves for failover, and status of each master. This can be useful for
alerting to conditions which prevent failover.

## SentinelGetMaster 

### What It Does

This method returns a IP:PORT connection string for the current master of the
given pod name.

### Why You Use It

This is used when the connecting client code simply wants to perform
master discovery. It calls this command to get the IP and PORT of the
current master to use in connecting to the master. It can also be used
in admin displays or tools.


## SentinelFailover

### What It Does

This method is used to *request* a given pod be failed over. .

### Why You Use It

You use this command to "manually' force a failover from the current
master of the pod to one of it's slaves. If the command is denied by
redis, which occurs when there are no available slaves to failover to,
it will return false with the reason.

Note this command returns as soon as the Sentinel server has accepted
it. It's return value does not indicate the failvoer has completed,
merely that it was initiation, nor the slave which was promoted

## SentinelMonitor

### What It Does

This method adds a new master/pod to the Sentinel instance for
management.

### Why You Use It

Whenever you need to add a new master to be managed by this Sentinel you
call this command. Sentinel will reject duplicates and the command will
return an error.

## SentinelRemove

### What It Does

This method is the counterpart to SentinelMonitor. It removes masters
from the Sentinel.

### Why You Use It

When you no longer need to monitor the given pod/master.

## SentinelSetPass

### What It Does

This method sets or changes the authpassword for a given pod/master.

### Why You Use It

If the pod/master you need to manage or monitor has the `requirepass`
directive set, you will need to let the Sentinel know. Note this does
*not* change the password on the master, just the one the Sentinel uses
to connect to the master (and slaves).

## SentinelSetString

### What It Does

This method sets a given Sentinel configuration variable for a specific
pod/master when the value being set is a string.

### Why You Use It

You may want some masters/pods to have different tolerances for master
unavailability. You would use this command to set those settings. 

## SentinelSetInt

### What It Does

This method sets a given Sentinel configuration variable for a specific
pod/master when the value being set is an integer.

### Why You Use It

You may want some masters/pods to have different tolerances for master
unavailability. You would use this command to set those settings. 
For example you can set the down-after-milliseconds setting to have a
given pod failed over in the event it is down for three seconds or 30
seconds.


# Exported Structures

## MasterAddress

This struct defines the addressing pair for a Sentinel Master. It is
used by the SentinelGetMaster method.

## MasterInfo

This struct contains all of the information Redis returns when asked
about master details such as via the SentinelMasters or SentinelMaster
methods.

The tags on struct members are used to identify and map the results from
Sentinel (which include names with dashes in them) to the struct.

## SlaveInfo

This struct contains all of the information Redis returns when asked
about slave details such as via the SentinelSlaves methods.

The tags on struct members are used to identify and map the results from
Sentinel (which include names with dashes in them) to the struct.
