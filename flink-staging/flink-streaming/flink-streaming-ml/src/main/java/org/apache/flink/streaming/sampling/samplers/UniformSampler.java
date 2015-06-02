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

import org.apache.flink.streaming.sampling.helpers.SamplingUtils;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by marthavk on 2015-06-02.
 */
public class UniformSampler<IN> implements SampleFunction<IN> {

	Buffer<IN> reservoir;
	long counter = 0;

	public UniformSampler(int lMaxSize) {
		reservoir = new Buffer<IN>(lMaxSize);
	}

	@Override
	public ArrayList<IN> getElements() {
		return reservoir.getSample();
	}

	@Override
	public void sample(IN element) {
		counter++;
		if (SamplingUtils.flip((double) reservoir.getMaxSize() / counter)) {
			if (!reservoir.isFull()) {
				reservoir.addSample(element);
			}
			else {
				replace(element);
			}
		}
	}

	@Override
	public IN getRandomEvent() {
		return reservoir.getSample().get(SamplingUtils.nextRandInt(reservoir.getMaxSize()));
	}

	@Override
	public void reset() {
		reservoir.getSample().clear();
		counter=0;

	}

	public void replace(IN item) {
		// choose position in sample uniformly at random
		int pos = new Random().nextInt(reservoir.getSize());
		// replace element at pos with item
		reservoir.replaceSample(pos, item);
	}

}
