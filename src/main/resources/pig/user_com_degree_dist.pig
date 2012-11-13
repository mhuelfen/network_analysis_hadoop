-- first node is always User
-- nodetype2

REGISTER ./src/main/resources/udf/ibmloader.jar;


-- parameters:
-- entity : User_entity file
-- maxtime :  maximum timestamp of log entries used for the calculation of the statistics


-- load data
user_entity = LOAD '$entity' USING eu.robust.wp2.networkanalysis.IbmDataLoader(';') AS (timestamp,nodeType1:chararray,nodeId1:chararray,rel:chararray,nodeType2:chararray,nodeId2:chararray,json:map[]);



-- filter by timestamp and check for null Ids
user_entity = filter user_entity by  timestamp <'$maxtime' and nodeId1 != 'null' and nodeId2 != 'null';

--1199263385000;USER;33909;JOINED;COMMUNITY;d4d1356b-655c-44b5-ae82-a40740140701;{"timestamp":1199263385000,"role":"owner"}
relation = filter user_entity by rel == 'JOINED' and nodeType1 == 'USER'  and nodeType2 == 'COMMUNITY';

describe relation;

-- group by node1
degrees1 = group relation by nodeId1;

-- sum in group to get node degree
degrees1 = foreach degrees1 generate flatten(group) as nodetype, COUNT(relation) as degree;

--dump degrees;
  
---- group by degree
dist1 = group degrees1 by degree;
dist1 = foreach dist1 generate '$maxtime' as week,'all' as community, group as degree, COUNT(degrees1) as count;
--dist2= foreach dist2 generate '$maxtime' as week, group.community as community,group.degree as degree, COUNT(degrees2) as count;

describe degrees1;

-- order to get increasing values
dist1 = order dist1 by count ASC;

STORE dist1 into './results/degreedist/degreedist1-USER-COMMUNITY-$maxtime';


--dist2= foreach dist2 generate '$maxtime' as week, group.community as community,group.degree as degree, COUNT(degrees2) as count;
--
---- order to get increasing values
--dist2 = order dist2 by community DESC,count ASC;
----
--STORE dist2 into './results/degreedist/degreedist2-$nodetype-$maxtime';
--dist2= foreach dist2 generate '$maxtime' as week, group.community as community,group.degree as degree, COUNT(degrees2) as count;

---- count to get degree count 
--
---- (calc degree prob n_k / n)
--
---- order to get degree dist
--
--
--
--
--
