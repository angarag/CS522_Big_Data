cd $HADOOP_PREFIX
bin/hadoop fs -mkdir mars
bin/hadoop fs -ls mars 
bin/hadoop fs -put $HADOOP_PREFIX/input1 mars
bin/hadoop fs -ls mars
bin/hadoop jar WordCount.jar com.mars.bigdata.hadoop.WordCount mars $HADOOP_PREFIX/output
bin/hadoop fs -ls $HADOOP_PREFIX/output 
bin/hadoop fs -cat $HADOOP_PREFIX/output/part-r-00000


