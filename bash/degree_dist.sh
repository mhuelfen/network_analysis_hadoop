#!/bin/bash

nodereltypes=(FORUM_THREAD REPLIED BLOG_ENTRY COMMENTED WIKI_PAGE EDITED FILE SHARED)

starttime=1177396027000
endtime=1304241701000
let week=86400*7*1000

time=$endtime
# del old log file
rm degreedist.log

while [ $time -gt $starttime ]; do
    for i in 0 2 4 6 
    do
	nodetype=${nodereltypes[i]}
	reltype=${nodereltypes[i+1]}
	echo working on week $time nodetype $nodetype reltype $reltype >> degreedist.log
    done

#    pig -x mapreduce -param maxtime=$time -param entity=hdfs:///mhuelfen/USER_ENTITY.csv pig/stats.pig 
    let time=time-week
	     
done



