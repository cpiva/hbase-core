#!/bin/bash

if [ $# -ne 1 ]; then
  echo "usage: $0 <hbase repositoryUri>"
  echo "e.g.:"
  echo "  $0 repo:hbase:zk"
  echo "  $0 repo:hbase:zk1,zk2,zk3"  
  exit 1
fi

echo "using repositoryUri: $1"

mvn -e kite:delete-dataset \
  -Dkite.repositoryUri=$1 \
  -Dkite.datasetName=party

mvn -e kite:delete-dataset \
  -Dkite.repositoryUri=$1 \
  -Dkite.datasetName=agreement

mvn -e kite:delete-dataset \
  -Dkite.repositoryUri=$1 \
 -Dkite.datasetName=party_agreement

echo "disable 'party';drop 'party'" | hbase shell
echo "disable 'agreement';drop 'agreement'" | hbase shell
echo "disable 'party_agreement';drop 'party_agreement'" | hbase shell
