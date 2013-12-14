hbase-core
==========

clone, compile and install the Cloudera CDK using:

git clone https://github.com/cloudera/cdk.git

mvn install -Dmaven.test.skip=true

then on hbase-core run the following:

mvn compile -Dmaven.test.skip=true

sh delete-dataset.sh (optional)

sh create-dataset.sh

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.WritePartyDataset"

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.ReadPartyDataset"

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.WriteAddressDataset"

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.ReadAddressDataset"

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.WriteEventDataset"

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.ReadEventDataset"

 
