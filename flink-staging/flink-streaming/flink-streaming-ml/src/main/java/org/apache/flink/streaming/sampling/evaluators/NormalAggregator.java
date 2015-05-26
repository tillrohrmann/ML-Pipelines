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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;

/**
 * Created by marthavk on 2015-05-20.
 */
public class NormalAggregator extends RichMapFunction<Double, GaussianDistribution> {
	SummaryStatistics aggr = new SummaryStatistics();

	@Override
	public GaussianDistribution map(Double value) throws Exception {
		aggr.addValue(value);
		return new GaussianDistribution(aggr.getMean(), aggr.getStandardDeviation());
	}
}
