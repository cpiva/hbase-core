HBASE_PARCELS_PATH=/opt/cloudera/parcels/CDH/lib/hbase
HBASE_USR_LIB_PATH=/usr/lib/hbase

if [ -d $HBASE_PARCELS_PATH ]; then
    HBASE_JAR_PATH=$HBASE_PARCELS_PATH/hbase.jar
else
    HBASE_JAR_PATH=$HBASE_USR_LIB_PATH/hbase.jar
fi

hadoop fs -rm -r -skipTrash agreement-out;
HADOOP_CLASSPATH=/etc/hbase/conf:$HBASE_JAR_PATH hadoop jar target/hbase-core-jar-with-dependencies.jar com.cloudera.cdk.hbase.mr.AgreementDriver -libjars $HBASE_JAR_PATH agreement agreement-out
