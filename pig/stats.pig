REGISTER ./udf/ibmloader.jar;

-- parameters:
-- user : User_User file
-- entity : User_entity file
-- maxtime :  maximum timestamp of log entries used for the calculation of the statistics

---- 1243849924000;USER;29638;REPLIED_FORUM_THREAD_OF;USER;206419;{"timestamp":1243849924000,"forum_id":"ce8a65c6-6749-4de1-85f0-1f8a82bfc9e4","reply_id":"29af9390-496a-44e2-b6cd-17d0995b4dd1"}
----user_user = LOAD './data/uupart.csv' USING eu.robust.wp2.networkanalysis.CustomLoader(';') AS (logtime,user1,id1,rel,user2,id2,json:map[]);
--user_user = LOAD '$user' USING eu.robust.wp2.networkanalysis.IbmDataLoader(';') AS (logtime,user1,id1,rel,user2,id2,json:map[]);
--
---- filter by timestamp, reply relation and ignore self replies
--replies = filter user_user by logtime < '$maxtime' and rel == 'REPLIED_FORUM_THREAD_OF' and id1 != id2;
--
---- select fields
--replies = FOREACH replies GENERATE json#'timestamp' as time,id1,id2,json#'forum_id' as threadId;--timestamp:double,u1:int,u2:int, thread:chararray	;
----


user_entity = LOAD '$entity' USING eu.robust.wp2.networkanalysis.IbmDataLoader(';') AS (timestamp,nodeType1:chararray,nodeId1,rel:chararray,nodeType2:chararray,nodeId2,json:map[]);

-- ?also filter for set of rel here?
-- filter by timestamp and check for null Ids
user_entity = filter user_entity by  timestamp < '$maxtime' and nodeId1 != 'null' and nodeId2 != 'null';

-- get community to thread relations
-- 1243845066000;FORUM_THREAD;ce8a65c6-6749-4de1-85f0-1f8a82bfc9e4;BELONGS_TO;COMMUNITY;2dad6e74-ceca-4eb0-a79a-a96d116b3d65;{"timestamp":1243845066000}
-- filter by timestamp, and communtiy relation and exclude null ids
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

-------
-- calculate in and out degree
-------
--comReplies = FOREACH comReplies GENERATE $0 as time ,$1 as poster:chararray,$2 as replier:chararray,$3 as threadId,$6 as commuId:chararray;
--
-- make replies unique
uniReplies = DISTINCT (FOREACH comReplies GENERATE community,creator,replier);

-- calculate in degree by grouping by community and creator and count the groups
in = GROUP uniReplies BY (community,creator);
in = FOREACH in GENERATE FLATTEN(group) as (community,creator) , COUNT(uniReplies);

--dump in;
--describe in;

-- calculate in degree by grouping by community and replier and count the groups
out = GROUP uniReplies BY (community,replier);
out = FOREACH out GENERATE FLATTEN(group) as (community,replier) , COUNT(uniReplies);

--dump out;
--describe out;

--STORE in into './results/in$maxtime';
--STORE out into './results/out$maxtime';


--------------
-- reciprocity
--------------
-- caculate times between creation and reply
replyTimes = foreach comReplies generate timeReply - timeCreate as deltaTime, creator, community;
-- group by creator and community
repro = GROUP replyTimes BY (community,creator);
repro = FOREACH repro GENERATE FLATTEN(group) as (community,replier) , SUM(replyTimes.deltaTime) / COUNT(replyTimes) as timeSum;

--describe repro;
--dump repro;


--------------
-- popularity
--------------
-- join creations with community
comCreates = JOIN creations BY thread, coms BY thread;
comCreates = foreach comCreates generate coms::thread as community, creations::creator as creator,coms::thread as thread;
popu = JOIN comCreates by thread LEFT OUTER , replies by thread;
popu = foreach popu generate $0..$3 as (community,creator,thread,replyTime);

popu = GROUP popu BY (community,creator);

popu = foreach popu generate group, SUM(replyTime);-- is null  ? 0 : 1
--
dump popu;
describe popu;