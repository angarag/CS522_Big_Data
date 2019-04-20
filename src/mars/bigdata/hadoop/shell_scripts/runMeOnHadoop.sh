JAR_NAME=$1
CLASS_NAME=$2
INPUT=$3

if [ $# -eq 0 ]
  then
    INPUT=input1
    JAR_NAME=Hadoop
    CLASS_NAME=WordCount
fi

cd $HADOOP_PREFIX
echo $JAR_NAME
echo $INPUT
bin/hadoop fs -mkdir mars
bin/hadoop fs -ls mars 
bin/hadoop fs -put $HADOOP_PREFIX/$INPUT mars
bin/hadoop fs -ls mars
bin/hadoop fs -rm -r -skipTrash $HADOOP_PREFIX/output
bin/hadoop jar $JAR_NAME.jar com.mars.bigdata.hadoop.$CLASS_NAME mars/$INPUT $HADOOP_PREFIX/output
bin/hadoop fs -ls $HADOOP_PREFIX/output 
bin/hadoop fs -cat $HADOOP_PREFIX/output/part-r-00000


