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

import com.cloudera.cdk.data.Key;
import com.cloudera.cdk.data.RandomAccessDataset;
import com.cloudera.cdk.hbase.data.avro.Agreement;

/**
 * Read the agreement objects from the agreements dataset by key lookup, and by
 * scanning.
 */
public class AgreementDatasetService extends AbstractHBaseService {
	public Agreement get(String id) throws Exception {
		// Load the addresses dataset
		RandomAccessDataset<Agreement> agreements = repo.load("agreement");

		// Get an accessor for the dataset and look up a agreement by id
		Key key = new Key.Builder(agreements).add("id", id).build();
		return agreements.get(key);

	}

}
