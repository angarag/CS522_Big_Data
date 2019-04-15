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

## How to run Spark project
