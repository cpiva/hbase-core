#!/bin/bash

if [ $# -ne 1 ]; then
  echo "usage: $0 <hbase repositoryUri>"
  echo "e.g.:"
  echo "  $0 repo:hbase:zk"
  echo "  $0 repo:hbase:zk1,zk2,zk3"  
  exit 1
fi

echo "using repositoryUri: $1"

mvn cdk:delete-dataset \
  -Dcdk.repositoryUri=$1 \
  -Dcdk.datasetName=party

mvn cdk:delete-dataset \
  -Dcdk.repositoryUri=$1 \
  -Dcdk.datasetName=event \

mvn cdk:delete-dataset \
  -Dcdk.repositoryUri=$1 \
  -Dcdk.datasetName=address \

mvn cdk:delete-dataset \
  -Dcdk.repositoryUri=$1 \
  -Dcdk.datasetName=party_address \
