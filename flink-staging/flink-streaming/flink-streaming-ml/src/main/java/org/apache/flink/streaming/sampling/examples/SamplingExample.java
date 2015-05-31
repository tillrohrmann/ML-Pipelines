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

package org.apache.flink.streaming.sampling.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.sampling.evaluators.NormalAggregator;
import org.apache.flink.streaming.sampling.generators.DoubleDataGenerator;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.sampling.samplers.Reservoir;
import org.apache.flink.streaming.sampling.samplers.ReservoirSampler;
import org.apache.flink.streaming.sampling.samplers.Sample;
import org.apache.flink.streaming.sampling.sources.NormalStreamSource;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Properties;
import java.util.Random;

/**
 * Created by marthavk on 2015-03-06.
 */
public class SamplingExample {

	public static long MAX_COUNT,TIME_WINDOW_SIZE;
	public static int COUNT_WINDOW_SIZE, SAMPLE_SIZE;
	public static double OUT_RATE;

	public static Properties initProps = new Properties();

	public static void main(String[] args) throws Exception {


		/*read properties file and set respective values*/
		initProps = SamplingUtils.readProperties(SamplingUtils.path + "distributionconfig.properties");
		MAX_COUNT = Long.parseLong(initProps.getProperty("maxCount"));
		COUNT_WINDOW_SIZE = Integer.parseInt(initProps.getProperty("countWindowSize"));
		TIME_WINDOW_SIZE = Long.parseLong(initProps.getProperty("timeWindowSize"));
		SAMPLE_SIZE = Integer.parseInt(initProps.getProperty("sampleSize"));
		OUT_RATE = Double.parseDouble(initProps.getProperty("outputRate"));

		/*set execution environment*/
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*sample the stream, run main algorithm*/

		/********* MAIN PROGRAM **********/
		/*create stream of distributions as source (also number generators) and shuffle*/
		DataStreamSource<GaussianDistribution> source = createSource(env);
		SingleOutputStreamOperator<GaussianDistribution, ?> shuffledSrc = source.shuffle();

		/*generate random number from distribution*/
		SingleOutputStreamOperator<Double, ?> generator =
				shuffledSrc.map(new DoubleDataGenerator<GaussianDistribution>());
		SingleOutputStreamOperator<GaussianDistribution, ?> aggregator = generator.map(new NormalAggregator());

		ReservoirSampler<Double> sampler = new ReservoirSampler<Double>(SAMPLE_SIZE, OUT_RATE);
		SingleOutputStreamOperator<Double, ?> sample = generator.flatMap(sampler);



		//sample.writeAsText(SamplingUtils.path + "reservoir_new");

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

}
