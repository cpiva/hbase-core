mvn cdk:delete-dataset \
  -Dcdk.repositoryUri=repo:hbase:localhost.localdomain \
  -Dcdk.datasetName=parties \
  -Dcdk.avroSchemaFile=src/main/avro/party.avsc

mvn cdk:delete-dataset \
  -Dcdk.repositoryUri=repo:hbase:localhost.localdomain \
  -Dcdk.datasetName=events \
  -Dcdk.avroSchemaFile=src/main/avro/event.avsc
