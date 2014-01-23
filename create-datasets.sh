#!/bin/bash

if [ $# -ne 1 ]; then
  echo "usage: $0 <hbase repositoryUri>"
  echo "e.g.:"
  echo "  $0 repo:hbase:zk"
  echo "  $0 repo:hbase:zk1,zk2,zk3"  
  exit 1
fi

echo "using repositoryUri: $1"

mvn kite:create-dataset \
  -Dkite.repositoryUri=$1 \
  -Dkite.datasetName=party \
  -Dkite.avroSchemaFile=src/main/avro/party.avsc

mvn kite:create-dataset \
  -Dkite.repositoryUri=$1 \
  -Dkite.datasetName=agreement \
  -Dkite.avroSchemaFile=src/main/avro/agreement.avsc    

mvn kite:create-dataset \
  -Dkite.repositoryUri=$1 \
  -Dkite.datasetName=party_agreement \
  -Dkite.avroSchemaFile=src/main/avro/party_agreement.avsc    
