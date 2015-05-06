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


import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.sampling.samplers.Sample;
import org.apache.flink.util.Collector;

public class KLDivergence extends RichCoFlatMapFunction<Sample<Double>, GaussianDistribution, Double> {
	GaussianDistribution currentDist = new GaussianDistribution();

	@Override
	public void flatMap1(Sample<Double> value, Collector<Double> out) throws Exception {
		GaussianDistribution sampledDist = new GaussianDistribution(value);
		out.collect(klDivergence(currentDist, sampledDist));
	}

	@Override
	public void flatMap2(GaussianDistribution value, Collector<Double> out) throws Exception {
		currentDist = value;
	}

	public double klDivergence(GaussianDistribution greal, GaussianDistribution gsampled) {
		//TODO
		return 0;

	}

}
