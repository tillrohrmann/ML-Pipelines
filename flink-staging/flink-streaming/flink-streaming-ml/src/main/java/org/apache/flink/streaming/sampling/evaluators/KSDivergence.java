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

/**
 * Created by marthavk on 2015-05-06.
 */

package org.apache.flink.streaming.sampling.evaluators;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.exception.InsufficientDataException;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.sampling.samplers.Buffer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * not tested! *
 */
public class KSDivergence extends RichCoFlatMapFunction<Buffer<Double>, GaussianDistribution, Double> {
	ArrayList<GaussianDistribution> trueAggregator = new ArrayList<GaussianDistribution>();
	ArrayList<Buffer<Double>> empAggregator = new ArrayList<Buffer<Double>>();

	@Override
	public void flatMap1(Buffer<Double> value, Collector<Double> out) throws Exception {
		empAggregator.add(value);

		if (trueAggregator.size() == empAggregator.size()) {
			for (int i = 0; i < trueAggregator.size(); i++) {
				double distance = ksDistance(trueAggregator.get(i), empAggregator.get(i));
				out.collect(distance);
			}
			trueAggregator.clear();
			empAggregator.clear();
		}

	}

	@Override
	public void flatMap2(GaussianDistribution value, Collector<Double> out) throws Exception {

		trueAggregator.add(value);
		if (trueAggregator.size() == empAggregator.size()) {
			for (int i = 0; i < trueAggregator.size(); i++) {
				double distance = ksDistance(trueAggregator.get(i), empAggregator.get(i));
				out.collect(distance);
			}
			trueAggregator.clear();
			empAggregator.clear();
		}
	}

	public double ksDistance(GaussianDistribution greal, Buffer<Double> gsampled) {
		KolmogorovSmirnovTest ksTest = new KolmogorovSmirnovTest();
		NormalDistribution real = new NormalDistribution(greal.getMean(), greal.getStandardDeviation());
		double[] sample = gsampled.getSampleAsArray();
		try {
			return ksTest.kolmogorovSmirnovTest(real, sample);
		} catch (InsufficientDataException e) {
			return Double.POSITIVE_INFINITY;
		}
	}


}
