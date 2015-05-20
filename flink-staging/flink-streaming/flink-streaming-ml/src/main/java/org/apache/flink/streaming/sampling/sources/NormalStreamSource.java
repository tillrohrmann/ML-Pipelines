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
package org.apache.flink.streaming.sampling.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.sampling.generators.NormalGenerator;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Created by marthavk on 2015-04-07.
 */
public class NormalStreamSource implements SourceFunction<Double> {

	long count = 0;
	NormalGenerator ng;
	Properties props;
	long numberOfEvents;

	public NormalStreamSource () {
		props = SamplingUtils.readProperties(SamplingUtils.path + "distributionconfig.properties");
		numberOfEvents = Long.parseLong(props.getProperty("maxCount"));
		ng = new NormalGenerator(props);

	}

	@Override
	public void run(Collector<Double> collector) throws Exception {

		for (count = 0; count < numberOfEvents; count ++) {
			ng.setCount(count);
			double generatedDouble = ng.generate();
			collector.collect(generatedDouble);
		}

	}

	@Override
	public void cancel() {

	}
}
