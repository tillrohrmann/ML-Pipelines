package org.apache.flink.streaming.sampling.examples;/*
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

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.fraction.Fraction;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.sampling.samplers.Reservoir;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Created by marthavk on 2015-05-08.
 */
public class Test {

	public static void main(String[] args) {



	}

	/**
	 * Windows the dataStream into windows of size wSize and constructs different reservoirs
	 * for each window. Then merges the reservoirs into one big reservoir.
	 *
	 * @param dataStream
	 * @param rSize
	 * @param wSize
	 */
	public static void windowSampling(DataStream<Long> dataStream, final Integer rSize, final Integer wSize) {
		dataStream
				.window(Count.of(wSize)).mapWindow(new WindowMapFunction<Long, Reservoir<Long>>() {
			@Override
			public void mapWindow(Iterable<Long> values, Collector<Reservoir<Long>> out) throws Exception {
				Reservoir<Long> r = new Reservoir<Long>(rSize);
				int count = 0;
				for (Long v : values) {
					count++;
					if (SamplingUtils.flip(count / rSize)) {
						r.addSample(v);
					}
				}
				out.collect(r);
			}
		})
				.flatten()
				.reduce(new ReduceFunction<Reservoir<Long>>() {
					@Override
					public Reservoir<Long> reduce(Reservoir<Long> value1, Reservoir<Long> value2) throws Exception {
						return Reservoir.merge(value1, value2);
						//return null;
					}
				}).print();
	}
}
