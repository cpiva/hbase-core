#!/bin/bash

if [ $# -ne 1 ]; then
  echo "usage: $0 <hbase repositoryUri>"
  echo "e.g.:"
  echo "  $0 repo:hbase:zk"
  echo "  $0 repo:hbase:zk1,zk2,zk3"  
  exit 1
fi

echo "using repositoryUri: $1"

mvn cdk:create-dataset \
  -Dcdk.repositoryUri=$1 \
  -Dcdk.datasetName=party \
  -Dcdk.avroSchemaFile=src/main/avro/party.avsc

mvn cdk:create-dataset \
  -Dcdk.repositoryUri=$1 \
  -Dcdk.datasetName=event \
  -Dcdk.avroSchemaFile=src/main/avro/event.avsc

mvn cdk:create-dataset \
  -Dcdk.repositoryUri=$1 \
  -Dcdk.datasetName=address \
  -Dcdk.avroSchemaFile=src/main/avro/address.avsc

mvn cdk:create-dataset \
  -Dcdk.repositoryUri=$1 \
  -Dcdk.datasetName=party_address \
  -Dcdk.avroSchemaFile=src/main/avro/party_address.avsc  