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

import org.apache.commons.math3.fraction.Fraction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Random;

/**
 * Performs uniform sampling with a reservoir. Each item in the stream has 1/maxSize final probability
 * to be included in the sample.
 * As the stream evolves, each new element is picked by probability equal to maxSize/counter where
 * counter is its position in the stream.
 * That means if the reservoir size hasn't reached maxSize, each element will be definitely picked.
 * If an item is picked and the reservoir is full then it replaces an existing element uniformly at
 * random.
 * @param <IN>
 */
public class ReservoirSampler<IN> implements FlatMapFunction<IN, IN>, Sampler<IN> {

	Fraction outputRate;
	Sample<IN> reservoir;
	//Reservoir reservoir;
	long counter = 0;
	long internalCounter = 0;

	public ReservoirSampler(int maxsize) {
		reservoir = new Sample<IN>(maxsize);
		outputRate = new Fraction(1);
	}

	public ReservoirSampler(int maxsize, double outR) {
		outputRate = new Fraction(outR);
		reservoir = new Sample<IN>(maxsize);
	}

	@Override
	public void flatMap(IN value, Collector<IN> out) throws Exception {
		counter++;
		internalCounter ++;
		sample(value);

		if (internalCounter == outputRate.getDenominator()) {
			internalCounter=0;
			for (int i=0; i<outputRate.getNumerator(); i++) {
				out.collect(reservoir.generate());
			}
		}
	}


	@Override
	public ArrayList<IN> getElements() {
		return reservoir.getSample();
	}

	@Override
	public void sample(IN element) {
		if (SamplingUtils.flip((double) reservoir.getMaxSize()/counter)) {
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


