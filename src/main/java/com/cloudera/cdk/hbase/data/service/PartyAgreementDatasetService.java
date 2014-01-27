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
package com.cloudera.cdk.hbase.data.service;

import java.util.ArrayList;
import java.util.List;

import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.Key;
import org.kitesdk.data.RandomAccessDataset;

import com.cloudera.cdk.hbase.data.avro.PartyAgreement;

/**
 * Read the party objects from the parties dataset by key lookup, and by scanning.
 */

public class PartyAgreementDatasetService extends AbstractHBaseService {
	
	RandomAccessDataset<PartyAgreement> partyAgreements = null;
	
	public PartyAgreementDatasetService(String repoUrl) {
		super(repoUrl);
	    // Load the dataset
		partyAgreements = repo.load("party_agreement");					
	}	

  public PartyAgreement get(String party_id, String agreement_id) throws Exception {

    // Get an accessor for the dataset and look up a party by id
    Key key = new Key.Builder(partyAgreements)
                    .add("party_id", party_id)
                    .add("agreement_id", agreement_id)
                    .build();
                    
    return partyAgreements.get(key);

  }

  public List<PartyAgreement>  scan(String partyId) throws Exception {
    List<PartyAgreement> ls=new ArrayList<PartyAgreement>(); 

    // Get a reader for the dataset and read all the users
    DatasetReader<PartyAgreement> reader = partyAgreements.newReader();
    try {
      reader.open();
      for (PartyAgreement partyAgreement : reader) {
        if(partyAgreement.getPartyId().toString().equals(partyId)){
          ls.add(partyAgreement);
        }
      }
    } finally {
      reader.close();
    }
    return ls;
  }
  //private boolean isPartyIdEqual(PartyAgreement partyAgreement, String partyId) {
	//  return partyAgreement.getPartyId().toString().startsWith(partyId);
  //}
}
