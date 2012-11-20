-- first node is always User

REGISTER udf/ibmloader.jar;
--REGISTER /home/mhuelfen/Documents/code/network_analysis_hadoop/src/main/resources/udf/ibmloader.jar;


-- parameters:
-- entity : User_entity file
-- nodetype the relation that is not user
-- reltype type of the relation
-- maxtime :  maximum timestamp of log entries used for the calculation of the statistics


-- load data
user_entity = LOAD '$entity' USING eu.robust.wp2.networkanalysis.IbmDataLoader(';') AS (timestamp,nodeType1:chararray,nodeId1:chararray,rel:chararray,nodeType2:chararray,nodeId2:chararray,json:map[]);


-- filter by timestamp and check for null Ids
user_entity = filter user_entity by  timestamp <'$maxtime' and nodeId1 != 'null' and nodeId2 != 'null';

--1199263385000;USER;33909;JOINED;COMMUNITY;d4d1356b-655c-44b5-ae82-a40740140701;{"timestamp":1199263385000,"role":"owner"}
relation = filter user_entity by rel == '$reltype' and nodeType1 == 'USER'  and nodeType2 == '$nodetype';
-- projection of needed fields
relation = foreach relation generate timestamp,nodeId1,nodeId2;

describe relation;

communities = filter user_entity by rel == 'BELONGS_TO' and nodeType1 == '$nodetype'  and nodeType2 == 'COMMUNITY';
communities = foreach communities generate nodeId1,nodeId2;
describe communities;
-- join relation with communities
relation_com = join relation by nodeId2, communities by nodeId1;

describe relation_com;
relation_com = foreach relation_com generate $0..$2 as (timestamp,nodeId1,nodeId2), communities::nodeId2 as community;
describe relation_com;

-- calculate degree dist for node1 -- node2

-- group by User Id
degrees1 = group relation_com by (nodeId1,community);

-- sum in group to get node degree
degrees1 = foreach degrees1 generate flatten(group) as (nodeId1,community), COUNT(relation_com) as degree;
--dump degrees1;
describe degrees1;
--  
---- group by degree
dist1 = group degrees1 by (degree,community);
--dist1 = foreach dist1 generate '$maxtime' as week, flatten(group) as (degree,community), COUNT(degrees1) as count;
dist1 = foreach dist1 generate '$maxtime' as week,  group.community as community,group.degree as degree, COUNT(degrees1) as count;

-- order to get increasing values
dist1 = order dist1 by community DESC,count ASC;
--


describe dist1;

--
----------
-- calculate degree dist for node1 -- node2
-- group by node1
degrees2 = group relation_com by (nodeId2,community);
describe degrees2;
-- sum in group to get node degree
degrees2 = foreach degrees2 generate flatten(group) as (nodeId2,community), COUNT(relation_com) as degree;
--dump degrees2;

describe degrees2;
--  
---- group by degree
dist2 = group degrees2 by (degree,community);
dist2= foreach dist2 generate '$maxtime' as week, group.community as community,group.degree as degree, COUNT(degrees2) as count;

-- order to get increasing values
dist2 = order dist2 by community DESC,count ASC;
--

----local
--STORE dist1 into './results/degreedist/degreedist1-USER-$nodetype-$maxtime';
--STORE dist2 into './results/degreedist/degreedist2-$nodetype-USER-$maxtime';
-- on cluster del old save results
rmr 'hdfs:///mhuelfen/results/degreedist/degreedist1-USER-$nodetype-$maxtime';
rmr 'hdfs:///mhuelfen/results/degreedist/degreedist2-$nodetype-USER-$maxtime';
STORE dist1 into 'hdfs:///mhuelfen/results/degreedist/degreedist1-USER-$nodetype-$maxtime';
STORE dist2 into 'hdfs:///mhuelfen/results/degreedist/degreedist2-$nodetype-USER-$maxtime';
