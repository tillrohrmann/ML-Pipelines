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
import java.util.Random;

/**
 * Created by marthavk on 2015-03-31.
 */
public class ReservoirSampler<IN> implements MapFunction<IN, Sample<IN>>, Sampler<IN> {

	Sample<IN> reservoir;
	//Reservoir reservoir;
	int count = 0;

	public ReservoirSampler(int size) {
		reservoir = new Sample<IN>(size);
		//reservoirSample = new Reservoir(size);
	}

	@Override
	public Sample<IN> map(IN value) throws Exception {
		count++;
		this.sample(value);
		return reservoir;
	}

	@Override
	public ArrayList<IN> getElements() {
		return reservoir.getSample();
	}

	@Override
	public void sample(IN element) {
		if (SamplingUtils.flip(count / reservoir.getMaxSize())) {
			if (!reservoir.isFull()) {
				reservoir.addSample(element);
			}
			else {
				replace(element);
			}
		}
	}


	public void replace(IN item) {
		// choose position in sample uniformly at random
		int pos = new Random().nextInt(reservoir.getSize());
		// replace element at pos with item
		reservoir.replaceSample(pos, item);
	}
}


