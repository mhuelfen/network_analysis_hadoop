#!/bin/bash

starttime=1177396027000
endtime=1304241701000
#endtime=1204241701000
let week=86400*7*1000

time=$endtime
# del old log file
rm stats.log

while [ $time -gt $starttime ]; do
             echo The counter is $time >> stats.log
#	     pig -x mapreduce -param maxtime=$time -param entity=hdfs:///mhuelfen/USER_ENTITY.csv pig/stats.pig
	     pig -x local -p user=../../data/ibm_network/USER_USER/flat/USER_USER.csv -p maxtime=$time ./src/main/resources/pig/user_user_in_out.pig
             let time=time-week
done
