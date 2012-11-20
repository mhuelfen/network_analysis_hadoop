REGISTER udf/ibmloader.jar;


-- parameters:
-- entity : User_entity file
-- maxtime :  maximum timestamp of log entries used for the calculation of the statistics

-- load data
user_entity = LOAD '$entity' USING eu.robust.wp2.networkanalysis.IbmDataLoader(';') AS (timestamp,nodeType1:chararray,nodeId1:chararray,rel:chararray,nodeType2:chararray,nodeId2:chararray,json:map[]);

-- filter by timestamp and check for null Ids
user_entity = filter user_entity by  timestamp <'$maxtime' and nodeId1 != 'null' and nodeId2 != 'null';

-- filter by relation
relation = filter user_entity by rel == 'JOINED' and nodeType1 == 'USER'  and nodeType2 == 'COMMUNITY';

-- group by node1 USER ID
degrees1 = group relation by nodeId1;

-- sum in group to get node degree
degrees1 = foreach degrees1 generate '$maxtime' as week, flatten(group) as nodeId, COUNT(relation) as degree, 'all' as community;

----------
-- other direction
----------

-- group by node1 USER ID
degrees2 = group relation by nodeId2;

-- sum in group to get node degree
degrees2 = foreach degrees2 generate '$maxtime' as week, flatten(group) as nodeId, COUNT(relation) as degree, 'all' as community;

---------------

-- store locally
--STORE degrees1 into './results/degrees/degrees-USER-COMMUNITY-$maxtime';
--STORE degrees2 into './results/degrees/degrees-COMMUNITY-USER-$maxtime';

STORE degrees1 into 'hdfs:///mhuelfen/results/degrees/degrees-USER-COMMUNITY-$maxtime';
STORE degrees2 into 'hdfs:///mhuelfen/results/degrees/degrees-COMMUNITY-USER-$maxtime';