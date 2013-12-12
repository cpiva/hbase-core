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

/**
 * Write some partyAgreement objects to the partyAgreement dataset using Avro specific records.
 */
public class WritePartyAgreementDataset extends Configured implements Tool {
	private static Logger logger = Logger.getLogger(WritePartyAgreementDataset.class);
	
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
    RandomAccessDataset<PartyAgreement> partyAgreements = repo.load("party_agreement");

    partyAgreements.put(partyAgreement("1", "1","1"));
    partyAgreements.put(partyAgreement("1", "2","2"));
    partyAgreements.put(partyAgreement("3", "4","4"));
    partyAgreements.put(partyAgreement("3", "5","5"));

    return 0;
  }

  private static PartyAgreement partyAgreement(String partyId, 
                                               String agreementId,
                                               String value) {
    return PartyAgreement.newBuilder()
        .setPartyId(partyId)
        .setAgreementId(agreementId)
        .setValue(value)
        .build();
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new WritePartyAgreementDataset(), args);
    System.exit(rc);
  }
}
