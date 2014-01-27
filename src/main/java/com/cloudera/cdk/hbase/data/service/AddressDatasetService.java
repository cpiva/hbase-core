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

import org.kitesdk.data.Key;
import org.kitesdk.data.RandomAccessDataset;

import com.cloudera.cdk.hbase.data.avro.Address;

/**
 * Read the address objects from the addresses dataset by key lookup, and by
 * scanning.
 */

public class AddressDatasetService extends AbstractHBaseService {

	public Address get(String id) throws Exception {
		// Load the addresses dataset
		RandomAccessDataset<Address> addresses = repo.load("address");

		// Get an accessor for the dataset and look up a address by id
		Key key = new Key.Builder(addresses).add("id", id).build();
		return addresses.get(key);

	}

}
