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

import com.cloudera.cdk.hbase.data.avro.Event;

/**
 * Read the event objects from the events dataset by key lookup, and by
 * scanning.
 */

public class EventDatasetService extends AbstractHBaseService {

	public Event get(String id) throws Exception {
		// Load the events dataset
		RandomAccessDataset<Event> events = repo.load("event");

		// Get an accessor for the dataset and look up a event by id
		Key key = new Key.Builder(events).add("id", id).build();
		return events.get(key);

	}

}
