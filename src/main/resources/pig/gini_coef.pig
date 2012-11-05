
-- filter by timestamp and check for null Ids
user_entity = filter user_entity by  timestamp <'$maxtime' and nodeId1 != 'null' and nodeId2 != 'null';

--1199263385000;USER;33909;JOINED;COMMUNITY;d4d1356b-655c-44b5-ae82-a40740140701;{"timestamp":1199263385000,"role":"owner"}
communities = filter user_entity by rel == 'JOINED' and nodeType1 == 'USER'  and nodeType2 == 'COMMUNITY';

describe communities;
-- group by