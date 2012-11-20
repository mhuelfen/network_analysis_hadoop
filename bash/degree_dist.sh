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
    echo working on week $time nodetype COMMUNITY reltype JOINED >> degreedist.log
    pig -x mapreduce -param maxtime=$time -param entity=hdfs:///mhuelfen/USER_ENTITY.csv pig/user_com_degree_dist.pig 
#    pig -x local -param maxtime=$time -param entity=../data/USER_ENTITY_2010-M06.csv ../src/main/resources/pig/user_com_degree_dist.pig
    
#    # rest of the relations
#    for i in 0 2 4 6 
#    do
#	nodetype=${nodereltypes[i]}
#	reltype=${nodereltypes[i+1]}
#	echo working on week $time nodetype $nodetype reltype $reltype >> degreedist.log
##	pig -x mapreduce -param maxtime=$time -param entity=hdfs:///mhuelfen/USER_ENTITY.csv -param reltype=$reltype -param nodetype=$nodetype pig/user_rel_degree_dists.pig
#	pig -x local -param maxtime=$time -param entity=../data/USER_ENTITY_2010-M06.csv -param reltype=$reltype -param nodetype=$nodetype ../src/main/resources/pig/user_rel_degree_dist.pig
#	exit
#    done
    let time=time-week
	     
done



