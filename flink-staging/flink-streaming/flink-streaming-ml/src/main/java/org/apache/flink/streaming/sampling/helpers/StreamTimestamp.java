/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.sampling.helpers;

import java.io.Serializable;

/**
 * Created by marthavk on 2015-04-24.
 */
public class StreamTimestamp implements Serializable {

	Long timestamp;

	public StreamTimestamp() {
		timestamp = System.currentTimeMillis();
	}

	public StreamTimestamp(int dd) {
		//suppose that year and month are the same and only day changes
		String date;
		if (dd<10) {
			date = 0 + "" + dd;
		}
		else {
			date = "" + dd;
		}
		timestamp = Long.parseLong(date);
	}

	public StreamTimestamp(long ts) {
		timestamp=ts;
	}


	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return timestamp.toString();
	}

	public void update () {
		timestamp = System.currentTimeMillis();
	}
}
