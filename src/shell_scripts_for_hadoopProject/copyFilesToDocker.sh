CONTAINER=$(docker ps -aqf "name=flamboyant_gagarin")
CONTAINER=$(docker ps | grep hadoop | awk '{print $1}')
echo $CONTAINER
JAR_NAME=$1
INPUT=$2

if [ $# -eq 0 ]
  then
    INPUT=input1
    JAR_NAME=WordCount
fi

docker cp $JAR_NAME.jar $CONTAINER:/usr/local/hadoop
docker cp $INPUT $CONTAINER:/usr/local/hadoop
docker cp runMeOnHadoop.sh $CONTAINER:/usr/local/hadoop

#docker run -it sequenceiq/hadoop-docker:latest /etc/bootstrap.sh -bash
#docker rm $(docker ps -a -q)


