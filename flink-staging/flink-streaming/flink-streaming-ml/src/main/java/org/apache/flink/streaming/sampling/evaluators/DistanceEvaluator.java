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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.examples.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.examples.sampling.samplers.Sample;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

/**
 * Created by marthavk on 2015-03-18.
 */
public class DistanceEvaluator extends RichCoFlatMapFunction<Sample<Double>, GaussianDistribution, Double>  {
	//TODO: Implement the Kullback-Leibler distance as metrics
	GaussianDistribution currentDist = new GaussianDistribution();

	@Override
	public void flatMap1(Sample<Double> value, Collector<Double> out) throws Exception {
		GaussianDistribution sampledDist = new GaussianDistribution(value);
		//System.out.println(currentDist.toString() + " " + sampledDist.toString());
		out.collect(bhattacharyyaDistance(currentDist, sampledDist));
	}

	@Override
	public void flatMap2(GaussianDistribution value, Collector<Double> out) throws Exception {
		currentDist = value;
	}

	public double bhattacharyyaDistance(GaussianDistribution greal, GaussianDistribution gsampled) {

		//Bhattacharyya distance
		double m1 = greal.getMean();
		double m2 = gsampled.getMean();
		double s1 = greal.getStandardDeviation();
		double s2 = gsampled.getStandardDeviation();

		double factor1 = Math.pow(s1, 2) / Math.pow(s2, 2) + Math.pow(s2, 2) / Math.pow(s1, 2) + 2;
		double factor2 = Math.pow((m1 - m2),2) / (Math.pow(s1,2) + Math.pow(s2,2));
		double distance = (0.25) * Math.log((0.25) * factor1) + (0.25) * factor2;
		return distance;

	}

	public void printIndexedString(String str, int subtaskIndex) {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		if (context.getIndexOfThisSubtask() == subtaskIndex) {
			System.out.println(str);
		}
	}

}
