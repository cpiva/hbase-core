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

import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.Key;
import com.cloudera.cdk.data.RandomAccessDataset;
import com.cloudera.cdk.hbase.data.avro.PartyAddress;

/**
 * Read the party objects from the parties dataset by key lookup, and by scanning.
 */

public class PartyAddressDatasetService extends AbstractHBaseService {

	RandomAccessDataset<PartyAddress> partyAddresses;
	
	public PartyAddressDatasetService(String repoUrl) {
		super(repoUrl);
	    // Load the parties dataset
	    partyAddresses = repo.load("party_address");					
	}


  public PartyAddress get(String party_id, String address_id) throws Exception {

    // Get an accessor for the dataset and look up a party by id
    Key key = new Key.Builder(partyAddresses)
                    .add("party_id", party_id)
                    .add("address_id", address_id)
                    .build();
                    
    return partyAddresses.get(key);

  }

  public List<PartyAddress>  scan(String partyId) throws Exception {
    List<PartyAddress> ls=new ArrayList<PartyAddress>(); 

    // Load the party_address dataset
    //RandomAccessDataset<PartyAddress> partyAddresses = repo.load("party_address");

    // Get a reader for the dataset and read all the users
    DatasetReader<PartyAddress> reader = partyAddresses.newReader();
    try {
      reader.open();
      for (PartyAddress partyAddress : reader) {
        if(partyAddress.getPartyId().toString().equals(partyId)){
          ls.add(partyAddress);
        }
      }
    } finally {
      reader.close();
    }
    return ls;
  }

}
