A = LOAD '/user/paepcke/Datasets/triplets.csv' USING PigStorage(',');
--A = LOAD 'Datasets/triplets.csv' USING PigStorage(',');
DUMP A;
B = ORDER A BY $0;
DUMP B;