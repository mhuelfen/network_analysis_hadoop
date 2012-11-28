-- load custom data loader
REGISTER ./src/main/resources/udf/ibmloader.jar;


-- parameters:
-- entity : User_entity file
-- maxtime :  maximum timestamp of log entries used for the calculation of the statistics

-- load data
user_entity = LOAD '$entity' USING eu.robust.wp2.networkanalysis.IbmDataLoader(';') AS (timestamp,nodeType1:chararray,nodeId1:chararray,rel:chararray,nodeType2:chararray,nodeId2:chararray,json:map[]);

-- ?also filter for set of rel here?
-- filter by timestamp and check for null Ids
user_entity = filter user_entity by  timestamp <'$maxtime' and nodeId1 != 'null' and nodeId2 != 'null';

-- get community to thread relations
-- 1243845066000;FORUM_THREAD;ce8a65c6-6749-4de1-85f0-1f8a82bfc9e4;BELONGS_TO;COMMUNITY;2dad6e74-ceca-4eb0-a79a-a96d116b3d65;{"timestamp":1243845066000}
-- filter by timestamp, and communtiy relation
coms = filter user_entity by rel == 'BELONGS_TO' and nodeType2 == 'COMMUNITY';
coms = foreach coms generate timestamp,nodeId1 as thread,nodeId2 as community;-- AS (timestamp:double, thread:chararray,community:chararray);

-- get user created thread relations
-- eg. 1243864123003;USER;2;CREATED;FORUM_THREAD;thread-3;{"timestamp":1243864123003}
creations = filter user_entity by rel == 'CREATED' and nodeType2 == 'FORUM_THREAD';
creations = foreach creations generate timestamp,nodeId1 as creator,nodeId2 as thread;
--describe creations;

-- get user replied thread relation
-- e.g. 1243864123011;USER;2;REPLIED;FORUM_THREAD;thread-1;{"timestamp":1243864123000,"reply_id":"reply-1"}
replies = filter user_entity by rel == 'REPLIED' and nodeType2 == 'FORUM_THREAD';
replies = foreach replies generate timestamp,nodeId1 as replier,nodeId2 as thread;
--describe replies;


-- join creations and replies using thread id
createReplies = join creations by thread, replies by thread;
--dump createReplies;
--describe createReplies;

--createReplies = foreach createReplies generate $0 as timeCreate, $3 as timeReply, as creator
createReplies = foreach createReplies generate creations::timestamp as timeCreate, replies::timestamp as timeReply, 
creations::creator as creator,replies::replier as replier,creations::thread as thread; 
--describe createReplies;

-- ignore self replies
createReplies = filter createReplies by creator != replier;

-- join creates&replies with community

comReplies = JOIN createReplies BY thread, coms BY thread;
comReplies = foreach comReplies generate $0..$4 as (timeCreate,timeReply,creator,replier,thread),coms::community as community;


-- join creations with community to get community for every thread
comCreates = JOIN creations BY thread, coms BY thread;
comCreates = foreach comCreates generate coms::community as community, creations::creator as creator,coms::thread as thread;

------------------------------
-- calculate in and out degree
------------------------------

-- make replies unique
uniReplies = DISTINCT (FOREACH comReplies GENERATE community,creator,replier);

-- calculate in degree by grouping by community and replier and count the groups
in = GROUP uniReplies BY (community,replier);
in = FOREACH out GENERATE '$maxtime' as maxtime:long,'in-degree' as label:chararray,FLATTEN(group) as (community,replier) , COUNT(uniReplies) as value;

-- calculate in degree by grouping by community and creator and count the groups
out = GROUP uniReplies BY (community,creator);
out = FOREACH out GENERATE '$maxtime' as maxtime:long,'out-degree' as label:chararray,FLATTEN(group) as (community,creator) , COUNT(uniReplies) as value;

--------------
-- reciprocity
--------------
-- calculate times between creation and reply
replyTimes = foreach comReplies generate timeReply - timeCreate as deltaTime, creator, community;
-- group by creator and community
repro = GROUP replyTimes BY (community,creator);
repro = FOREACH repro GENERATE FLATTEN(group) as (community,creator) , SUM(replyTimes.deltaTime) / COUNT(replyTimes) as reprocity;
---- join with creations to set infinity value for creators never replied to
repro = JOIN comCreates by (community,creator) LEFT OUTER, repro by (community,creator); 

-- bring into right dataformat
repro = FOREACH repro GENERATE '$maxtime' as maxtime:long,'reprocity' as label:chararray,
					 $0..$1 as (community:chararray, creator:chararray),
					 (repro::reprocity is null ? -1 : repro::reprocity) as value:double;

--------------
-- popularity
--------------

 -- get a tuple for each replied thread
wasReplied= DISTINCT (FOREACH comReplies GENERATE community,creator,thread);

-- group by comm. and user to count number of replied thread
numReplied = GROUP wasReplied BY (community,creator);
numReplied = FOREACH numReplied GENERATE FLATTEN(group) as (community,creator), COUNT(wasReplied) as sumReplied;

-- get number of created thread per comm. and user
numCreated = GROUP comCreates by (community,creator);
numCreated = FOREACH numCreated GENERATE FLATTEN(group) as (community,creator),COUNT(comCreates)*1.0 as sumCreated:double; 

-- join numbers of replied and creates thread to calculate replied/created = popularity
popu = JOIN numReplied by (community,creator) RIGHT OUTER, numCreated by (community,creator);
popu = FOREACH popu GENERATE '$maxtime' as maxtime:long,'popularity' as label:chararray,$3..$4 as (community,creator), (numReplied::sumReplied is null ? 0 : numReplied::sumReplied /  numCreated::sumCreated) as value;

----store localy
--STORE in into './results/in/in$maxtime';
--STORE out into './results/out/out$maxtime';
--STORE repro into './results/repro/test_repro$maxtime';
--STORE popu into './results/popu/popu$maxtime';

---- cluster del old save results
--rmr  'hdfs:///mhuelfen/results/in/in$maxtime'
--rmr 'hdfs:///mhuelfen/results/out/out$maxtime'
--rmr 'hdfs:///mhuelfen/results/repro/repro$maxtime'
--rmr 'hdfs:///mhuelfen/results/popu/popu$maxtime'
--STORE in into 'hdfs:///mhuelfen/results/in/in$maxtime';
--STORE out into 'hdfs:///mhuelfen/results/out/out$maxtime';
--STORE repro into 'hdfs:///mhuelfen/results/repro/repro$maxtime';
--STORE popu into 'hdfs:///mhuelfen/results/popu/popu$maxtime';

--describe in;
--describe out;
--describe popu;
--describe repro;