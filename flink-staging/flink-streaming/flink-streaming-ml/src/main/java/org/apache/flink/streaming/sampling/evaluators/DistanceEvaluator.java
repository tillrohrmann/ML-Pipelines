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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.sampling.samplers.Sample;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Created by marthavk on 2015-03-18.
 */
public class DistanceEvaluator extends RichCoFlatMapFunction<Sample<Double>, GaussianDistribution, Double>  {
	ArrayList<GaussianDistribution> trueAggregator = new ArrayList<GaussianDistribution>();
	ArrayList<GaussianDistribution> empAggregator = new ArrayList<GaussianDistribution>();

	@Override
	public void flatMap1(Sample<Double> value, Collector<Double> out) throws Exception {
		SummaryStatistics stats = SamplingUtils.getStats(value);
		GaussianDistribution sampledDist = new GaussianDistribution(stats.getMean(), stats.getStandardDeviation());
		empAggregator.add(sampledDist);

		if (trueAggregator.size() == empAggregator.size()) {
			for (int i=0; i<trueAggregator.size(); i++) {
				out.collect(SamplingUtils.bhattacharyyaDistance(empAggregator.get(i), trueAggregator.get(i)));
			}
			trueAggregator.clear();
			empAggregator.clear();
		}

	}

	@Override
	public void flatMap2(GaussianDistribution value, Collector<Double> out) throws Exception {

		trueAggregator.add(value);

		if (trueAggregator.size() == empAggregator.size()) {
			for (int i=0; i<trueAggregator.size(); i++) {
				out.collect(SamplingUtils.bhattacharyyaDistance(empAggregator.get(i), trueAggregator.get(i)));
			}
			trueAggregator.clear();
			empAggregator.clear();
		}
	}

	public void printIndexedString(String str, int subtaskIndex) {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		if (context.getIndexOfThisSubtask() == subtaskIndex) {
			System.out.println(str);
		}
	}






}
