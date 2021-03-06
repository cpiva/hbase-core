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

import java.util.Date;
import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.RandomAccessDataset;
import com.cloudera.cdk.data.RandomAccessDatasetRepository;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Write some address objects to the addresses dataset using Avro specific records.
 */
public class WriteAddressDataset extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    // Construct an HBase dataset repository using the local HBase database
    RandomAccessDatasetRepository repo =
        DatasetRepositories.openRandomAccess("repo:hbase:localhost.localdomain");

    // Load the address dataset
    RandomAccessDataset<Address> addresses = repo.load("address");

    // Get an accessor for the dataset and write some addresses to it
    long x = System.currentTimeMillis();

    addresses.put(address("1",x,x,"party1_address1","1"));
    addresses.put(address("2",x,x,"party1_address2","2"));
    addresses.put(address("3",x,x,"party1_address3","3"));
    addresses.put(address("4",x,x,"party2_address1","4"));
    addresses.put(address("5",x,x,"party2_address2","5"));

    return 0;
  }

  private static Address address(String id, 
                                  long startDttm, long endDttm, 
                                   String addressRole, String eventId) {
    return Address.newBuilder()
        .setId(id)
        .setStartDttm(startDttm)
        .setEndDttm(endDttm)
        .setAddressRole(addressRole)
        .setEventId(eventId)
        .build();
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new WriteAddressDataset(), args);
    System.exit(rc);
  }
}
