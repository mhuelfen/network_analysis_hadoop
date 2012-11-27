-- load custom data loader
REGISTER ./src/main/resources/udf/ibmloader.jar;

-- bad line user_user 1819591


-- parameters:
-- user : User_entity file
-- maxtime :  maximum timestamp of log entries used for the calculation of the statistics


-- load data
--user_user = LOAD '$user' USING eu.robust.wp2.networkanalysis.IbmDataLoader(';') AS (timestamp,nodeType1:chararray,nodeId1:chararray,rel:chararray,nodeType2:chararray,nodeId2:chararray,json:map[]);
user_user = LOAD '$user' USING eu.robust.wp2.networkanalysis.IbmDataLoader(';') AS (timestamp,nodeType1:chararray,nodeId1:chararray,rel:chararray,nodeType2:chararray,nodeId2:chararray,json);



-- filter by timestamp and check for null Ids
user_user = filter user_user by  timestamp <'$maxtime' and nodeId1 != 'null' and nodeId2 != 'null' and json#'community_id' != 'null';
--
--
-- get user1, user2 and community 
user_user = foreach user_user generate nodeId1 as user1,nodeId2 as user2,json#'community_id' as community;

in = GROUP user_user BY (community,user2);
in = FOREACH in GENERATE '$maxtime' as maxtime:long,'in-degree' as label:chararray,FLATTEN(group) as (community,creator) , COUNT(user_user) as value;


-- calculate in degree by grouping by community and replier and count the groups
out = GROUP user_user BY (community,user1);
out = FOREACH out GENERATE '$maxtime' as maxtime:long,'out-degree' as label:chararray,FLATTEN(group) as (community,replier) , COUNT(user_user) as value;

--describe out;
STORE in into './results/test/user_user_in$maxtime';
STORE out into './results/test/user_user_out$maxtime';

--STORE in into './results/user_user/user_user_in$maxtime';
--STORE out into './results/user_user/user_user_out$maxtime';


--
---- get community to thread relations
---- 1243845066000;FORUM_THREAD;ce8a65c6-6749-4de1-85f0-1f8a82bfc9e4;BELONGS_TO;COMMUNITY;2dad6e74-ceca-4eb0-a79a-a96d116b3d65;{"timestamp":1243845066000}
---- filter by timestamp, and communtiy relation
--coms = filter user_entity by rel == 'BELONGS_TO' and nodeType2 == 'COMMUNITY';
--coms = foreach coms generate timestamp,nodeId1 as thread,nodeId2 as community;-- AS (timestamp:double, thread:chararray,community:chararray);
--
---- get user created thread relations
---- eg. 1243864123003;USER;2;CREATED;FORUM_THREAD;thread-3;{"timestamp":1243864123003}
--creations = filter user_entity by rel == 'CREATED' and nodeType2 == 'FORUM_THREAD';
--creations = foreach creations generate timestamp,nodeId1 as creator,nodeId2 as thread;
----describe creations;
--
---- get user replied thread relation
---- e.g. 1243864123011;USER;2;REPLIED;FORUM_THREAD;thread-1;{"timestamp":1243864123000,"reply_id":"reply-1"}
--replies = filter user_entity by rel == 'REPLIED' and nodeType2 == 'FORUM_THREAD';
--replies = foreach replies generate timestamp,nodeId1 as replier,nodeId2 as thread;
----describe replies;
--
--
---- join creations and replies using thread id
--createReplies = join creations by thread, replies by thread;
----dump createReplies;
----describe createReplies;
--
----createReplies = foreach createReplies generate $0 as timeCreate, $3 as timeReply, as creator
--createReplies = foreach createReplies generate creations::timestamp as timeCreate, replies::timestamp as timeReply, 
--creations::creator as creator,replies::replier as replier,creations::thread as thread; 
----describe createReplies;
--
---- ignore self replies
--createReplies = filter createReplies by creator != replier;
--
---- join creates&replies with community
--
--comReplies = JOIN createReplies BY thread, coms BY thread;
--comReplies = foreach comReplies generate $0..$4 as (timeCreate,timeReply,creator,replier,thread),coms::community as community;
--
--
---- join creations with community to get community for every thread
--comCreates = JOIN creations BY thread, coms BY thread;
--comCreates = foreach comCreates generate coms::community as community, creations::creator as creator,coms::thread as thread;
--
---------
---- calculate in and out degree
---------
--
---- make replies unique
--uniReplies = DISTINCT (FOREACH comReplies GENERATE community,creator,replier);
--
---- calculate in degree by grouping by community and creator and count the groups
--in = GROUP uniReplies BY (community,creator);
--in = FOREACH in GENERATE '$maxtime' as maxtime:long,'in-degree' as label:chararray,FLATTEN(group) as (community,creator) , COUNT(uniReplies) as value;
--
----dump in;
----describe in;
--
---- calculate in degree by grouping by community and replier and count the groups
--out = GROUP uniReplies BY (community,replier);
--out = FOREACH out GENERATE '$maxtime' as maxtime:long,'out-degree' as label:chararray,FLATTEN(group) as (community,replier) , COUNT(uniReplies) as value;
--
--
------store localy
----STORE in into './results/in/in$maxtime';
----STORE in into './results/out/out$maxtime';
--
--
--
----
--
------ cluster del old save results
----rmr  'hdfs:///mhuelfen/results/in/in$maxtime'
----rmr 'hdfs:///mhuelfen/results/out/out$maxtime'
--
----STORE in into 'hdfs:///mhuelfen/results/in/in$maxtime';
----STORE in into 'hdfs:///mhuelfen/results/out/out$maxtime';
--
--
--
--
----describe in;
----describe out;
