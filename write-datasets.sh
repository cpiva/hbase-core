#!/bin/bash

if [ $# -ne 1 ]; then
  echo "usage: $0 <hbase repositoryUri>"
  echo "e.g.:"
  echo "  $0 repo:hbase:zk"
  echo "  $0 repo:hbase:zk1,zk2,zk3"  
  exit 1
fi

echo "using repositoryUri: $1"

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.WritePartyDataset" -Dexec.args=$1

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.WriteAddressDataset" -Dexec.args=$1

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.WritePartyAddressDataset" -Dexec.args=$1

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.WriteEventDataset" -Dexec.args=$1

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.WriteAgreementDataset" -Dexec.args=$1

mvn exec:java -Dexec.mainClass="com.cloudera.cdk.hbase.data.WritePartyAgreementDataset" -Dexec.args=$1