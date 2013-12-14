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
 * Write some party objects to the parties dataset using Avro specific records.
 */
public class WritePartyDataset extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    // Construct an HBase dataset repository using the local HBase database
    RandomAccessDatasetRepository repo =
        DatasetRepositories.openRandomAccess("repo:hbase:localhost.localdomain");

    // Load the party dataset
    RandomAccessDataset<Party> parties = repo.load("party");

    // Get an accessor for the dataset and write some parties to it
    long x = System.currentTimeMillis();

    parties.put(party("1", "desc1","type1",x,x,"status_code1","status_desc1","lang1","1"));
    parties.put(party("2", "desc2","type2",x,x,"status_code2","status_desc2","lang2","2"));
    parties.put(party("3", "desc3","type3",x,x,"status_code3","status_desc3","lang3","3"));
    parties.put(party("4", "desc4","type4",x,x,"status_code4","status_desc4","lang4","4"));
    parties.put(party("5", "desc5","type5",x,x,"status_code5","status_desc5","lang5","5"));

    return 0;
  }

  private static Party party(String id, String desc, String type, 
                             long startDttm, long endDttm, 
                             String statusCode, String statusDesc, 
                             String languageCode, String eventId) {
    return Party.newBuilder()
        .setId(id)
        .setDesc(desc)
        .setType(type)
        .setStartDttm(startDttm)
        .setEndDttm(endDttm)
        .setStatusCode(statusCode)
        .setStatusDesc(statusDesc)
        .setLanguageCode(languageCode)
        .setEventId(eventId)
        .build();
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new WritePartyDataset(), args);
    System.exit(rc);
  }
}
