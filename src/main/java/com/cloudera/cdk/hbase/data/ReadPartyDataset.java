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

/**
 * Read the party objects from the parties dataset by key lookup, and by scanning.
 */
public class ReadPartyDataset extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    // Construct an HBase dataset repository using the local HBase database
    RandomAccessDatasetRepository repo =
        DatasetRepositories.openRandomAccess("repo:hbase:localhost.localdomain");

    // Load the party dataset
    RandomAccessDataset<Party> parties = repo.load("party");

    // Get an accessor for the dataset and look up a party by id
    Key key = new Key.Builder(parties).add("id", "1").build();
    Key key2 = new Key.Builder(parties).add("id", "9").build();
    System.out.println(parties.get(key));
    System.out.println(parties.get(key2));
    
    // Get a reader for the dataset and read all the users
    DatasetReader<Party> reader = parties.newReader();
    try {
      reader.open();
      for (Party party : reader) {
        System.out.println(party);
      }
    } finally {
      reader.close();
    }

    return 0;
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new ReadPartyDataset(), args);
    System.exit(rc);
  }
}
