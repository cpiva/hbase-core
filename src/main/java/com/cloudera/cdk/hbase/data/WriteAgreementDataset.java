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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.RandomAccessDataset;
import com.cloudera.cdk.data.RandomAccessDatasetRepository;
import com.cloudera.cdk.hbase.data.avro.Agreement;

/**
 * Write some agreement objects to the agreement dataset using Avro specific records.
 */
public class WriteAgreementDataset extends Configured implements Tool {
	private static Logger logger = Logger.getLogger(WriteAgreementDataset.class);

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

    // Load the address dataset
    RandomAccessDataset<Agreement> agreements = repo.load("agreement");

    // Get an accessor for the dataset and write some addresses to it
    long x = System.currentTimeMillis();

    agreements.put(agreement("1",x,x,"party1_agreement1","RS1","reason1","party1_agreement1","checking","CAD","Canadian Dollars","S1","Marketing","TD Marketing","1"));
    agreements.put(agreement("2",x,x,"party1_agreement2","RS1","reason1","party1_agreement2","savings","CAD","Canadian Dollars","S1","Marketing","TD Marketing","2"));
    agreements.put(agreement("3",x,x,"party1_agreement3","RS1","reason1","party1_agreement3","money market","CAD","Canadian Dollars","S1","Marketing","TD Marketing","3"));
    agreements.put(agreement("4",x,x,"party2_agreement1","RS1","reason1","party2_agreement1","checking","CAD","Canadian Dollars","S1","Marketing","TD Marketing","4"));
    agreements.put(agreement("5",x,x,"party2_agreement2","RS1","reason1","party2_agreement2","savings","CAD","Canadian Dollars","S1","Marketing","TD Marketing","5"));

    return 0;
  }

  private static Agreement agreement(String id, 
                                   long startDttm, long endDttm, 
                                   String desc, String reasonCode,
                                   String reasonText, String description,
                                   String typeDesc, String currencyCode,
                                   String currencyDesc, String statusCode,
                                   String lobCategory, String lobName,
                                   String eventId) {
    return Agreement.newBuilder()
        .setId(id)
        .setStartDttm(startDttm)
        .setEndDttm(endDttm)
        .setDesc(desc)
        .setReasonCode(reasonCode)
        .setReasonText(reasonText)
        .setDescription(description)
        .setTypeDesc(typeDesc)
        .setCurrencyCode(currencyCode)
        .setCurrencyDesc(currencyDesc)
        .setStatusCode(statusCode)
        .setLobCategory(lobCategory)
        .setLobName(lobName)
        .setEventId(eventId)
        .build();
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new WriteAgreementDataset(), args);
    System.exit(rc);
  }
}
