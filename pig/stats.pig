REGISTER ./udf/ibmloader.jar;

-- parameters:
-- user : User_User file
-- entity : User_entity file
-- maxtime :  maximum timestamp of log entries used for the calculation of the statistics

-- 1243849924000;USER;29638;REPLIED_FORUM_THREAD_OF;USER;206419;{"timestamp":1243849924000,"forum_id":"ce8a65c6-6749-4de1-85f0-1f8a82bfc9e4","reply_id":"29af9390-496a-44e2-b6cd-17d0995b4dd1"}
--user_user = LOAD './data/uupart.csv' USING eu.robust.wp2.networkanalysis.CustomLoader(';') AS (logtime,user1,id1,rel,user2,id2,json:map[]);
user_user = LOAD '$user' USING eu.robust.wp2.networkanalysis.IbmDataLoader(';') AS (logtime,user1,id1,rel,user2,id2,json:map[]);

-- filter by timestamp, reply relation and ignore self replies
replies = filter user_user by logtime < '$maxtime' and rel == 'REPLIED_FORUM_THREAD_OF' and id1 != id2;

-- select fields
replies = FOREACH replies GENERATE json#'timestamp' as time,id1,id2,json#'forum_id' as threadId;--timestamp:double,u1:int,u2:int, thread:chararray	;
--
-- 1243845066000;FORUM_THREAD;ce8a65c6-6749-4de1-85f0-1f8a82bfc9e4;BELONGS_TO;COMMUNITY;2dad6e74-ceca-4eb0-a79a-a96d116b3d65;{"timestamp":1243845066000}
user_entity = LOAD '$entity' USING eu.robust.wp2.networkanalysis.IbmDataLoader(';') AS (logtime,node1,threadId,rel,node2,commuId,json:map[]);

-- filtzrt by timestamp, and communtiy relation and exclude null ids
coms = filter user_entity by logtime < '$maxtime' and rel == 'BELONGS_TO' and node2 == 'COMMUNITY' and commuId != 'null';
coms = foreach coms generate json#'timestamp' as time,threadId,commuId;-- AS (timestamp:double, thread:chararray,community:chararray);

--dump coms;
-- join user user & user entity
-- result  0:timestamp reply, 1:creator id, 2:replier id,3:thread id, 4:timestamp thread to community, 5:thread id, 6:community id
comReplies = JOIN replies BY threadId, coms BY threadId;
comReplies = FOREACH comReplies GENERATE $0 as time ,$1 as poster:chararray,$2 as replier:chararray,$3 as threadId,$6 as commuId:chararray;

-- make replies unique
uni = DISTINCT (FOREACH comReplies GENERATE commuId,poster,replier);

repNum = GROUP uni BY (commuId,poster);
in = FOREACH repNum GENERATE FLATTEN(group) as (commuId,poster) , COUNT(uni);

repNum2 = GROUP uni BY (commuId,replier);
out = FOREACH repNum GENERATE FLATTEN(group) as (commuId,replier) , COUNT(uni);

STORE in into './results/in$maxtime';
--STORE out into './results/out$maxtime';


--DESCRIBE in;
--DESCRIBE out;


--dump comReplies;
--
----describe replies;
--describe comReplies;