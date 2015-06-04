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
import org.apache.flink.streaming.sampling.generators.DoubleDataGenerator;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.sampling.sources.NormalStreamSource;

import java.util.Properties;

/**
 * Created by marthavk on 2015-06-03.
 */
public class DistributionComparison {

	public static long MAX_COUNT,TIME_WINDOW_SIZE;
	public static int COUNT_WINDOW_SIZE, SAMPLE_SIZE;


	public static Properties initProps = new Properties();

	public static void main(String[] args) throws Exception {

		/*set up properties*/
		readProperties();

		/*set execution environment*/
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		/*create debug source*/
		//DataStreamSource<Long> debugSource = env.addSource(new DebugSource(500000));

		/** OR **/

		/*create stream of distributions as source (also number generators) and shuffle*/
		DataStreamSource<GaussianDistribution> source = createSource(env);
		//SingleOutputStreamOperator<GaussianDistribution, ?> shuffledSrc = source.shuffle();

		/*generate random number from distribution*/
		SingleOutputStreamOperator<Double, ?> doubleStream =
				source.map(new DoubleDataGenerator<GaussianDistribution>());


		/*create samplers*/
		UniformSampler<Double> uniformSampler = new UniformSampler<Double>(SAMPLE_SIZE,10);
		PrioritySampler<Double> prioritySampler = new PrioritySampler<Double>(SAMPLE_SIZE,TIME_WINDOW_SIZE,1000);
		ChainSampler<Double> chainSampler = new ChainSampler<Double>(SAMPLE_SIZE,COUNT_WINDOW_SIZE,1000);
		FiFoSampler<Double> fiFoSampler = new FiFoSampler<Double>(SAMPLE_SIZE,100);
		BiasedReservoirSampler<Double> biasedReservoirSampler = new BiasedReservoirSampler<Double>(SAMPLE_SIZE,100);
		GreedySampler<Double> greedySampler = new GreedySampler<Double>(SAMPLE_SIZE,100);

		/*sample*/
		doubleStream.transform("sample", doubleStream.getType(), new StreamSampler<Double>(prioritySampler));
		doubleStream.transform("sample", doubleStream.getType(), new StreamSampler<Double>(uniformSampler));
		doubleStream.transform("sample", doubleStream.getType(), new StreamSampler<Double>(chainSampler));
		doubleStream.transform("sample", doubleStream.getType(), new StreamSampler<Double>(fiFoSampler));
		doubleStream.transform("sample", doubleStream.getType(), new StreamSampler<Double>(greedySampler));
		doubleStream.transform("sample", doubleStream.getType(), new StreamSampler<Double>(biasedReservoirSampler));
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

	public static void readProperties() {
		initProps = SamplingUtils.readProperties(SamplingUtils.path + "distributionconfig.properties");
		MAX_COUNT = Long.parseLong(initProps.getProperty("maxCount"));
		COUNT_WINDOW_SIZE = Integer.parseInt(initProps.getProperty("countWindowSize"));
		TIME_WINDOW_SIZE = Long.parseLong(initProps.getProperty("timeWindowSize"));
		SAMPLE_SIZE = Integer.parseInt(initProps.getProperty("sampleSize"));
	}


}
