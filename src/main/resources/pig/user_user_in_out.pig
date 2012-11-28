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
