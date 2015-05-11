package org.apache.flink.streaming.sampling.samplers;/*
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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.incrementalML.inspector.ChangeDetector;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;

import java.util.ArrayList;

/**
 * Created by marthavk on 2015-05-11.
 * Greedy sampler uses a change detection component in order to detect concept drift
 * in the stream. If such a change is detected then a certain percentage of tuples
 * from the reservoir are evicted and new ones are sampled using the biased reservoir sampling
 * algorithm.
 */
public class GreedySampler<IN> implements MapFunction<IN, Sample<IN>>, Sampler<IN> {

	Reservoir reservoirSample;
	ChangeDetector detector = new ChangeDetector();
	private boolean hasDrift =false;
	double bias;
	int count=0;

	public GreedySampler(int size) {
		reservoirSample = new Reservoir(size);
	}

	@Override
	public Sample<IN> map(IN value) throws Exception {
		count++;
		detector.input((Double)value);
		hasDrift = detector.isChangedDetected();
		if (hasDrift) {
			reservoirSample.discard(0.5);
			hasDrift = false;
			detector.reset();
		}

		double proportion = reservoirSample.getSize()/reservoirSample.getMaxSize();
		if (SamplingUtils.flip(proportion)) {
			reservoirSample.replaceSample(value);
		}
		else {
			reservoirSample.addSample(value);
		}
		return null;
	}

	@Override
	public ArrayList<IN> getElements() {
		return null;
	}

	@Override
	public void sample(IN element) {

	}

	public void insertBias(double bias) {

	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public int maxSize() {
		return 0;
	}
}
