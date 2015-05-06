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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.sampling.samplers.ChainSample;
import org.apache.flink.streaming.sampling.samplers.PrioritySampler;
import org.apache.flink.streaming.sampling.samplers.Sample;
import org.apache.flink.streaming.sampling.samplers.Sampler;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import java.util.ArrayList;

/**
 * Created by marthavk on 2015-04-23.
 */
public class SampleExtractor<T> extends RichMapFunction<ChainSample<Tuple3<T,StreamTimestamp,Long>>, Sample<T>> {

	public SampleExtractor() {
		super();
	}

	@Override
	public Sample<T> map(ChainSample<Tuple3<T, StreamTimestamp, Long>> chain) throws Exception {
		//ArrayList<Long> utilList = new ArrayList<Long>();
		Sample<T> sample = new Sample<T>();
		for (int i=0; i<chain.getSize(); i++) {
			if(chain.get(i).peekFirst()!=null) {
				//utilList.add(chain.get(i).peekFirst().f2);
				sample.addSample(chain.get(i).peekFirst().f0);
			}
		}
		//printIndexedString(utilList.toString(),0);
		return sample;
	}

	void printIndexedString(String str, int subtaskIndex) {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		if (context.getIndexOfThisSubtask() == subtaskIndex) {
			System.out.println(str);
		}
	}

}


