register s3n://uw-cse344-code/myudfs.jar

raw = LOAD 's3://uw-cse344' USING TextLoader as (line:chararray);

ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);
subjects = group ntriples by (subject) PARALLEL 50;

--Count the number of times a subject appears in the list
count_by_subject = foreach subjects generate flatten($0), COUNT($1) as count PARALLEL 50;

--Group subjects and their counts by the count
cocgroup = group count_by_subject by (count) PARALLEL 50;

--Generate the number of counts of a particular subject
cocos = foreach cocgroup generate flatten($0) as counta, COUNT($1) as coc PARALLEL 50;

-- store the results in the folder /user/hadoop/problem2-results
store cocos into '/user/hadoop/problem2-results' using PigStorage();


--ec2-52-10-200-199.us-west-2.compute.amazonaws.com
--hadoop dfs -getmerge /user/hadoop/problem2-results problem2-results
--scp -o "ServerAliveInterval 10" -i CSE544Homework3.pem -r hadoop@ec2-52-10-200-199.us-west-2.compute.amazonaws.com:problem2-results .
