
-- filter by timestamp and check for null Ids
user_entity = filter user_entity by  timestamp <'$maxtime' and nodeId1 != 'null' and nodeId2 != 'null';

--1199263385000;USER;33909;JOINED;COMMUNITY;d4d1356b-655c-44b5-ae82-a40740140701;{"timestamp":1199263385000,"role":"owner"}
communities = filter user_entity by rel == 'JOINED' and nodeType1 == 'USER'  and nodeType2 == 'COMMUNITY';

describe communities;


-- (join with communities)

-- group by node1
degrees = group communities by nodeId1;


-- sum in group to get node degree
degrees = foreach degrees generate flatten(group) as nodetype, COUNT(communities) as degree;

--dump degrees;
describe degrees;
  
---- group by degree
dist = group degrees by degree;
dist = foreach dist generate group as degreename, COUNT(degrees);
--
dump dist;
describe dist;
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
