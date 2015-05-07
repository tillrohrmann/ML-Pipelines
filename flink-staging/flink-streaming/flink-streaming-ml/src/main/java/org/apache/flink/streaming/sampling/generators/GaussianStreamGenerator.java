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
package org.apache.flink.streaming.sampling.generators;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Created by marthavk on 2015-04-07.
 */
public class GaussianStreamGenerator implements SourceFunction<GaussianDistribution> {

	long count = 0;
	GaussianDistribution gaussD;
	Properties props;
	long numberOfEvents, steps;
	double mean, stDev, meanStep, stDevStep, meanInit, stDevInit, meanTarget,  stDevTarget;


	public GaussianStreamGenerator(Properties lProps) {
		props = lProps;

		/*parse properties*/
		meanInit = Double.parseDouble(props.getProperty("meanInit"));
		stDevInit = Double.parseDouble(props.getProperty("stDevInit"));
		meanTarget = Double.parseDouble(props.getProperty("meanTarget"));
		stDevTarget = Double.parseDouble(props.getProperty("stDevTarget"));

		/*create initial normal distribution*/
		mean=meanInit;
		stDev=stDevInit;
		gaussD = new GaussianDistribution(mean, stDev) ;

		numberOfEvents = Long.parseLong(props.getProperty("maxCount"));

		boolean isSmooth = Boolean.parseBoolean(props.getProperty("isSmooth"));
		if (!isSmooth && steps<=(numberOfEvents/2)) {
			steps = Long.parseLong(props.getProperty("numberOfSteps"));
		}
		else {
			steps = numberOfEvents;
		}


		meanStep = (meanTarget - mean) / (steps-1);
		stDevStep = (stDevTarget - stDev) / (steps-1);
	}

	@Override
	public void run(Collector<GaussianDistribution> collector) throws Exception {

		while (count < numberOfEvents) {
			count++;

			collector.collect(gaussD);

			double multiplier = Math.floor(count*steps/numberOfEvents);
			mean = meanInit + meanStep*multiplier;
			stDev = stDevInit + stDevStep*multiplier;
			gaussD = new GaussianDistribution(mean, stDev);
		}

	}

	@Override
	public void cancel() {

	}
}
