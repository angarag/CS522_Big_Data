# CS522_Big_Data
Here you can find my assignments and group projects for the Big Data course taught in April, 2019
## Technology
MapReduce, Hadoop, Spark, SparkSQL, Scala

## How to run Hadoop project
The followings are used in this project:
* Docker version: v18
* Hadoop image for docker: https://hub.docker.com/r/sequenceiq/hadoop-docker
* Eclipse (for Scala) as IDE:
* JDK 1.7 configured for Eclipse
* Hadoop 2.7 jars added for Eclipse build path

1) Generate JAR from Eclipse
2) Run Hadoop image for docker by using runDocker.sh
3) Copy JAR and input parameter files to docker by using copyFilesToDocker.sh
4) Run runMeOnHadoop.sh on docker image bash

### How to run a program:

Let’s assume we need to run the Word Count algorithm: 
Copy the jar to docker by running the relevant shell: 

```bash
./copyFilesToDocker.sh Hadoop input1 
```

In the above case, input1 is a text file with some words. Run the program on docker: 

```bash
bash-4.1# cd $HADOOP_PREFIX 
bash-4.1# chmod +x runMeOnHadoop.sh  
bash-4.1# ./runMeOnHadoop.sh Hadoop WordCount input1 
bash-4.1# ./runMeOnHadoop.sh Hadoop InMapperWordCount input1 //the number of input records for Reducer is less compared to the regular Word Count algorithm. 
angarag@angarag-Inspiron-5442:~$ ./copyFilesToDocker.sh Hadoop access_log //you can repeat the same operation for the input files 
bash-4.1# ./runMeOnHadoop.sh Hadoop AverageProblem access_log 
bash-4.1# ./runMeOnHadoop.sh Hadoop InMapperAverageProblem access_log //we expect the communication cost would be reduced. 
bash-4.1# ./runMeOnHadoop.sh Hadoop Pair input2 
bash-4.1# ./runMeOnHadoop.sh Hadoop Stripes input2 //the number of input records for Reducer is less than Pair approach. 
bash-4.1# ./runMeOnHadoop.sh Hadoop PairStripes input2 //there is a huge memory consumption in Reducer part for PairStripes problem. 
```

Please note that I have overrode the cleanup method for inMapperAverageProblem and PairStripes to emit the hashmap at the end. 
I emitted hashmaps for the mentioned algorithms above. So the output is not human-readable. You may want to look at the stdout log files. 

## How to run Spark project

