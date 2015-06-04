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
package org.apache.flink.streaming.sampling.samplers;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.sampling.evaluators.NormalAggregator;
import org.apache.flink.streaming.sampling.generators.DoubleDataGenerator;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.sampling.sources.DebugSource;
import org.apache.flink.streaming.sampling.sources.NormalStreamSource;

import java.util.Properties;

/**
 * Created by marthavk on 2015-05-08.
 */
public class SamplingExample {

	public static long MAX_COUNT,TIME_WINDOW_SIZE;
	public static int COUNT_WINDOW_SIZE, SAMPLE_SIZE;
	public static double OUT_RATE;

	public static Properties initProps = new Properties();

	public static void main(String[] args) throws Exception {

		/*set up properties*/
		readProperties();

		/*set execution environment*/
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*create debug source*/
		DataStreamSource<Long> debugSource = env.addSource(new DebugSource(1000000));

		/** OR **/

		/*create stream of distributions as source (also number generators) and shuffle*/
		DataStreamSource<GaussianDistribution> source = createSource(env);
		SingleOutputStreamOperator<GaussianDistribution, ?> shuffledSrc = source.shuffle();

		/*generate random number from distribution*/
		SingleOutputStreamOperator<Double, ?> generator =
				shuffledSrc.map(new DoubleDataGenerator<GaussianDistribution>());
		SingleOutputStreamOperator<GaussianDistribution, ?> aggregator = generator.map(new NormalAggregator());

		/*create samplers*/
		UniformSampler<Long> uniformSampler = new UniformSampler<Long>(10,10);
		PrioritySampler<Long> prioritySampler = new PrioritySampler<Long>(10,100,1000);
		ChainSampler<Long> chainSampler = new ChainSampler<Long>(10,100,1000);
		FiFoSampler<Long> fiFoSampler = new FiFoSampler<Long>(10,100);
		BiasedReservoirSampler<Long> biasedReservoirSampler = new BiasedReservoirSampler<Long>(10,100);

		/*sample*/
		debugSource.transform("sample", debugSource.getType(), new StreamSampler<Long>(biasedReservoirSampler)).print();

		/*get js for execution plan*/
		System.err.println(env.getExecutionPlan());

		/*execute program*/
		env.execute();

	}

	/**
	 * Creates a DataStreamSource of GaussianDistribution items out of the params at input.
	 *
	 * @param env the StreamExecutionEnvironment.
	 * @return the DataStreamSource
	 */
	public static DataStreamSource<GaussianDistribution> createSource(StreamExecutionEnvironment env) {
		return env.addSource(new NormalStreamSource());
	}


	public static void foo() {
/*		int sampleRate = 3500;
		double l = 1000.0/sampleRate;

		double number = 0.0000019;

		long decimal = (long)number;
		//int fractional = Integer.parseInt(doubleAsText.split("\\.")[1]);
		int fractional = (int) Math.round((number - decimal) * 1000000);
		System.out.println(number);
		System.out.println(decimal);
		System.out.println(fractional);*/
	}

	public static void readProperties() {
		initProps = SamplingUtils.readProperties(SamplingUtils.path + "distributionconfig.properties");
		MAX_COUNT = Long.parseLong(initProps.getProperty("maxCount"));
		COUNT_WINDOW_SIZE = Integer.parseInt(initProps.getProperty("countWindowSize"));
		TIME_WINDOW_SIZE = Long.parseLong(initProps.getProperty("timeWindowSize"));
		SAMPLE_SIZE = Integer.parseInt(initProps.getProperty("sampleSize"));
		OUT_RATE = Double.parseDouble(initProps.getProperty("outputRate"));
	}



}
