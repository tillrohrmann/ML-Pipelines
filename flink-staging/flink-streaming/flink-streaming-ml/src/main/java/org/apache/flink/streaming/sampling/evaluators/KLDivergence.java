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


import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.sampling.samplers.Sample;
import org.apache.flink.util.Collector;

public class KLDivergence extends RichCoFlatMapFunction<Sample<Double>, NormalDistribution, Double> {
	NormalDistribution currentDist = new NormalDistribution();

	@Override
	public void flatMap1(Sample<Double> value, Collector<Double> out) throws Exception {
		SummaryStatistics stats = SamplingUtils.getStats(value);
		NormalDistribution sampledDist = new NormalDistribution(stats.getMean(), stats.getStandardDeviation());
		out.collect(klDivergence(currentDist, sampledDist));
	}

	@Override
	public void flatMap2(NormalDistribution value, Collector<Double> out) throws Exception {
		currentDist = value;
	}

	public double klDivergence(NormalDistribution greal, NormalDistribution gsampled) {
		//TODO
		return 0;

	}

	private double additive(NormalDistribution p, NormalDistribution q, int x) {
		return p.density(x) * Math.log(q.density(x));
	}

}
