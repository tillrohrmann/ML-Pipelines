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
	long stablePoints;
	double mean, stDev, meanStep, stDevStep, meanInit, stDevInit, meanTarget,  stDevTarget, outlierRate;


	public GaussianStreamGenerator(Properties lProps) {
		props = lProps;

		/*parse properties*/
		meanInit = Double.parseDouble(props.getProperty("meanInit"));
		stDevInit = Double.parseDouble(props.getProperty("stDevInit"));
		meanTarget = Double.parseDouble(props.getProperty("meanTarget"));
		stDevTarget = Double.parseDouble(props.getProperty("stDevTarget"));
		outlierRate = Double.parseDouble(props.getProperty("outlierRate"));


		/*create initial normal distribution*/
		mean=meanInit;
		stDev=stDevInit;
		gaussD = new GaussianDistribution(mean, stDev, outlierRate) ;

		numberOfEvents = Long.parseLong(props.getProperty("maxCount"));

		boolean isSmooth = Boolean.parseBoolean(props.getProperty("isSmooth"))&& steps<=(numberOfEvents/2);

		if (!isSmooth) {
			steps = Long.parseLong(props.getProperty("numberOfSteps"));
			stablePoints = 0;
			meanStep = (meanTarget - mean) / (steps-1);
			stDevStep = (stDevTarget - stDev) / (steps-1);
		}
		else {
			steps = numberOfEvents-2*stablePoints;
			stablePoints = Long.parseLong(props.getProperty("stablePoints"));
			meanStep = (meanTarget - mean) / (steps);
			stDevStep = (stDevTarget - stDev) / (steps);
		}

	}

	@Override
	public void run(Collector<GaussianDistribution> collector) throws Exception {

		for (count=0; count<stablePoints; count++) {
			gaussD = new GaussianDistribution(mean, stDev, outlierRate);
			collector.collect(gaussD);
		}

		for (count=stablePoints; count<numberOfEvents-stablePoints; count++) {
			long interval = numberOfEvents-2*stablePoints;
			long countc = count-stablePoints;
			double multiplier = Math.floor(countc * steps / interval);
			mean = meanInit + meanStep * multiplier;
			stDev = stDevInit + stDevStep * multiplier;
			gaussD = new GaussianDistribution(mean, stDev, outlierRate);
			collector.collect(gaussD);
		}

		for (count=numberOfEvents-stablePoints; count<numberOfEvents; count++) {
			gaussD = new GaussianDistribution(mean, stDev, outlierRate);
			collector.collect(gaussD);
		}
	}

	@Override
	public void cancel() {

	}
}
