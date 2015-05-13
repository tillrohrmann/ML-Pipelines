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
package org.apache.flink.streaming.sampling.evaluators;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.sampling.samplers.Sample;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;


/**
 * Created by marthavk on 2015-04-27.
 */
public class DistributionComparator implements CoFlatMapFunction<Sample<Double>, GaussianDistribution,
		Tuple2<GaussianDistribution, Integer>> {
	//GaussianDistribution currentDist = new GaussianDistribution();
	@Override
	public void flatMap1(Sample<Double> value, Collector<Tuple2<GaussianDistribution,Integer>> out) throws Exception {
		SummaryStatistics stats = SamplingUtils.getStats(value);
		GaussianDistribution sampledDist = new GaussianDistribution(stats.getMean(), stats.getStandardDeviation());
		out.collect(new Tuple2<GaussianDistribution, Integer>(sampledDist, SamplingUtils.EMPIRICAL_DISTRIBUTION));
	}

	@Override
	public void flatMap2(GaussianDistribution value, Collector<Tuple2<GaussianDistribution,Integer>> out)
			throws Exception {
		out.collect(new Tuple2<GaussianDistribution, Integer>(value, SamplingUtils.REAL_DISTRIBUTION));
		//currentDist = value;
	}


}
