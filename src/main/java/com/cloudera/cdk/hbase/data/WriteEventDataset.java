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
 * Write some event objects to the events dataset using Avro specific records.
 */
public class WriteEventDataset extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    // Construct an HBase dataset repository using the local HBase database
    RandomAccessDatasetRepository repo =
        DatasetRepositories.openRandomAccess("repo:hbase:localhost.localdomain");

    // Load the event dataset
    RandomAccessDataset<Event> events = repo.load("event");

    // Get an accessor for the dataset and write some events to it
    long x = System.currentTimeMillis();

    events.put(event("1","source_system_code1",x,"details_text1","status1","1"));
    events.put(event("2","source_system_code2",x,"details_text2","status2","2"));
    events.put(event("3","source_system_code3",x,"details_text3","status3","3"));

    return 0;
  }

  private static Event event(String id, String sourceSystemCode, long eventDttm, 
                             String detailsText, String status, String userId) {
    return Event.newBuilder()
        .setId(id)
        .setSourceSystemCode(sourceSystemCode)
        .setEventDttm(eventDttm)
        .setDetailsText(detailsText)
        .setStatus(status)
        .setUserId(userId)
        .build();
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new WriteEventDataset(), args);
    System.exit(rc);
  }
}
