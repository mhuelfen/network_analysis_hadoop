#!/bin/bash

nodereltypes=(FORUM_THREAD REPLIED BLOG_ENTRY COMMENTED WIKI_PAGE EDITED FILE SHARED)

starttime=1177396027000
endtime=1304241701000
let week=86400*7*1000

time=$endtime
# del old log file
rm degreedist.log

while [ $time -gt $starttime ]; do
    # user - community
    echo working on week $time nodetype COMMUNITY reltype JOINED >> degree.log
    pig -x mapreduce -param maxtime=$time -param entity=hdfs:///mhuelfen/USER_ENTITY.csv pig/user_com_degrees.pig 
    let time=time-week
done