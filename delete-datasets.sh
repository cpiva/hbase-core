mvn cdk:delete-dataset \
  -Dcdk.repositoryUri=repo:hbase:localhost.localdomain \
  -Dcdk.datasetName=party \
  -Dcdk.avroSchemaFile=src/main/avro/party.avsc

mvn cdk:delete-dataset \
  -Dcdk.repositoryUri=repo:hbase:localhost.localdomain \
  -Dcdk.datasetName=event \
  -Dcdk.avroSchemaFile=src/main/avro/event.avsc

mvn cdk:delete-dataset \
  -Dcdk.repositoryUri=repo:hbase:localhost.localdomain \
  -Dcdk.datasetName=address \
  -Dcdk.avroSchemaFile=src/main/avro/address.avsc

mvn cdk:delete-dataset \
  -Dcdk.repositoryUri=repo:hbase:localhost.localdomain \
  -Dcdk.datasetName=party_address \
  -Dcdk.avroSchemaFile=src/main/avro/party_address.avsc    