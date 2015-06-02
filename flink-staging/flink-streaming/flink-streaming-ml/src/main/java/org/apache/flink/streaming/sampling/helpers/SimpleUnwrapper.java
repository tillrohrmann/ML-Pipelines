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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.sampling.samplers.Buffer;

/**
 * Created by marthavk on 2015-04-27.
 */
public class SimpleUnwrapper<T> extends RichMapFunction<Buffer<Tuple3<T, StreamTimestamp, Long>>, Buffer<T>> {
	@Override
	public Buffer<T> map(Buffer<Tuple3<T, StreamTimestamp, Long>> value) throws Exception {
		//ArrayList<Long> utilList = new ArrayList<Long>();
		Buffer<T> sample = new Buffer<T>();
		for (int i = 0; i < value.getSample().size(); i++) {
			sample.addSample(value.getSample().get(i).f0);
		}
		return sample;
	}
}
