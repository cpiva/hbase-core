/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.hbase.data;

import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.Key;
import com.cloudera.cdk.data.RandomAccessDataset;
import com.cloudera.cdk.data.RandomAccessDatasetRepository;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.hbase.avro.SpecificAvroDao;
import com.cloudera.cdk.data.hbase.impl.Dao;
import com.cloudera.cdk.data.hbase.impl.EntityScanner;
import com.cloudera.cdk.data.hbase.impl.SchemaManager;
import com.cloudera.cdk.data.hbase.manager.DefaultSchemaManager;
import com.cloudera.cdk.data.hbase.tool.SchemaTool;
import com.cloudera.cdk.data.hbase.avro.AvroUtils;
/**
 * Read the partyAddress objects from the party_addresses dataset by key lookup, and by scanning.
 */
public class ReadPartyAddressDataset extends Configured implements Tool {
	Logger logger = Logger.getLogger(WriteAddressDataset.class);

  @Override
  public int run(String[] args) throws Exception {
	  if (args.length < 1)
	  {
		  logger.error("Please pass the HBase repo URI in the form repo:hbase:zk1,zk2,zk3");
		  throw new IllegalArgumentException("HBase URI is requred: repo:hbase:zk1,zk2,zk3");
	  }
	  else 
		  logger.info("using hbase repo URI: " + args[0]);
	  
    // Construct an HBase dataset repository
    RandomAccessDatasetRepository repo =
        DatasetRepositories.openRandomAccess(args[0]);

    // Load the party_address dataset
    RandomAccessDataset<PartyAddress> partyAddresses = repo.load("party_address");

    // Get an accessor for the dataset and look up a partyAddress 
    Key key = new Key.Builder(partyAddresses)
        .add("party_id", "1")
        .add("address_id", "1").build();

    System.out.println(partyAddresses.get(key));


    // Get a reader for the dataset and read all the users
    DatasetReader<PartyAddress> reader = partyAddresses.newReader();
    try {
      reader.open();
      for (PartyAddress partyAddress : reader) {
        System.out.println(partyAddress);
      }
    } finally {
      reader.close();
    }

    return 0;
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new ReadPartyAddressDataset(), args);
    System.exit(rc);
  }
}
