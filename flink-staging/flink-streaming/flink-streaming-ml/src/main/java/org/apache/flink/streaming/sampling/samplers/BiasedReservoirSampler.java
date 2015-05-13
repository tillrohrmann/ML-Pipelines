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

package org.apache.flink.streaming.sampling.samplers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;

import java.util.ArrayList;


/**
 * Created by marthavk on 2015-04-01.
 */
public class BiasedReservoirSampler<IN> implements MapFunction<IN, Sample<IN>>, Sampler<IN> {

	Reservoir reservoirSample;
	int count = 0;

	public BiasedReservoirSampler(int size) {
		reservoirSample = new Reservoir(size);
	}

	@Override
	public Sample<IN> map(IN value) throws Exception {
		count++;
		sample(value);
		return reservoirSample;
	}


	@Override
	public ArrayList<IN> getElements() {
		return reservoirSample.getSample();
	}

	@Override
	public void sample(IN element) {
		double proportion = reservoirSample.getSize() / reservoirSample.getMaxSize();
		if (SamplingUtils.flip(proportion)) {
			reservoirSample.replaceSample(element);
		} else {
			reservoirSample.addSample(element);
		}
	}

	@Override
	public int size() {
		return reservoirSample.getSize();
	}

	@Override
	public int maxSize() {
		return reservoirSample.getMaxSize();
	}
}
