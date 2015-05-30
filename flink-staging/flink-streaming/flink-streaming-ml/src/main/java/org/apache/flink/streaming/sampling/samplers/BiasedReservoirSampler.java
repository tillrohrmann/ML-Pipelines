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
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.util.Collector;

import java.util.ArrayList;


/**
 * Created by marthavk on 2015-04-01.
 */
public class BiasedReservoirSampler<IN> implements FlatMapFunction<IN, IN>, Sampler<IN> {

	Reservoir<IN> reservoir;
	int counter = 0;
	Fraction outputRate;
	long internalCounter=0;

	public BiasedReservoirSampler(int size) {
		reservoir = new Reservoir<IN>(size);
		outputRate = new Fraction(1);
	}

	public BiasedReservoirSampler(int size, double outR) {
		reservoir = new Reservoir<IN>(size);
		outputRate = new Fraction(outR);
	}

	@Override
	public void flatMap(IN value, Collector<IN> out) throws Exception {
		internalCounter++;
		counter++;
		sample(value);
		if (internalCounter==outputRate.getDenominator()) {
			internalCounter=0;
			for (int i=0; i<outputRate.getNumerator(); i++) {
				out.collect((IN) reservoir.generate());
			}
		}

	}

	@Override
	public ArrayList<IN> getElements() {
		return reservoir.getSample();
	}

	@Override
	public void sample(IN element) {
		double proportion = reservoir.getSize() / reservoir.getMaxSize();
		if (SamplingUtils.flip(proportion)) {
			reservoir.replaceSample(element);
		} else {
			reservoir.addSample(element);
		}
	}



}
