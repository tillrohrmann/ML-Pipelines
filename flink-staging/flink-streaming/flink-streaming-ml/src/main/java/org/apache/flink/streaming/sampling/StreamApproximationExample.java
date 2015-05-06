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

package org.apache.flink.streaming.sampling;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.sampling.evaluators.DistanceEvaluator;
import org.apache.flink.streaming.sampling.evaluators.DistributionComparator;
import org.apache.flink.streaming.sampling.generators.DataGenerator;
import org.apache.flink.streaming.sampling.generators.GaussianDistribution;
import org.apache.flink.streaming.sampling.generators.GaussianStreamGenerator;
import org.apache.flink.streaming.sampling.helpers.SampleExtractor;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.sampling.helpers.SimpleUnwrapper;
import org.apache.flink.streaming.sampling.helpers.StreamTimestamp;
import org.apache.flink.streaming.sampling.samplers.*;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Created by marthavk on 2015-03-13.
 */
public class StreamApproximationExample {
	public static String path = "flink-staging/flink-streaming/flink-streaming-examples/src/main/resources/";
	public static long MAX_COUNT;  // max count of generated numbers
	public static int COUNT_WINDOW_SIZE;
	public static long TIME_WINDOW_SIZE; //In milliseconds
	public static int SAMPLE_SIZE;
	public static Properties initProps = new Properties();

	// *************************************************************************
	// PROGRAM
	// *************************************************************************
	public static void main(String args[]) throws Exception {

		/*read properties file and set static variables*/
		initProps = SamplingUtils.readProperties(path + "distributionconfig.properties");
		MAX_COUNT = Long.parseLong(initProps.getProperty("maxCount"));
		COUNT_WINDOW_SIZE = Integer.parseInt(initProps.getProperty("countWindowSize"));
		TIME_WINDOW_SIZE = Long.parseLong(initProps.getProperty("timeWindowSize"));
		SAMPLE_SIZE = Integer.parseInt(initProps.getProperty("sampleSize"));

		/*set execution environment*/
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*evaluate sampling method, run main algorithm*/
		evaluateSampling(env, initProps);

		/*get js for execution plan*/
		System.err.println(env.getExecutionPlan());

		/*execute program*/
		env.execute();

	}

	/**
	 * Evaluates the sampling method. Compares final sample distribution parameters
	 * with source.
	 * @param env
	 * @param initProps
	 */
	public static void evaluateSampling(StreamExecutionEnvironment env, final Properties initProps) {

		int defaultParallelism = Runtime.getRuntime().availableProcessors();
		int sampleSize = SAMPLE_SIZE;

		/*create source*/
		DataStreamSource<GaussianDistribution> source = createSource(env, initProps);

		/*generate random numbers according to Distribution parameters*/



		SingleOutputStreamOperator<GaussianDistribution,?> operator = source.shuffle()

				/*generate double value from GaussianDistribution and wrap around
				Tuple3<Double, Timestamp, Long> */
				.map(new MapFunction<GaussianDistribution, GaussianDistribution>() {
					@Override
					public GaussianDistribution map(GaussianDistribution value) throws Exception {
						return value;
					}
				});

				operator.map(new DataGenerator())

				/*sample the stream*/
				//.map(new PrioritySampler<Double>(sampleSize, TIME_WINDOW_SIZE))
				//.map(new ChainSampler<Double>(sampleSize, COUNT_WINDOW_SIZE))
				//.map(new ReservoirSampler<Tuple3<Double, StreamTimestamp, Long>>(sampleSize))
				//.map(new BiasedReservoirSampler<Tuple3<Double, StreamTimestamp, Long>>(sampleSize))
				.map(new FifoSampler<Tuple3<Double, StreamTimestamp, Long>>(sampleSize))
				//.setParallelism(1)

				/*extract Double sampled values (unwrap from Tuple3)*/
				.map(new SimpleUnwrapper<Double>())
				//.setParallelism(1)

				/*connect sampled stream to source*/
				.connect(operator)

				/*evaluate sample: compare current distribution parameters with sampled distribution parameters*/
				.flatMap(new DistanceEvaluator())

				//.setParallelism(1)

				//sink
				//.print();
				.writeAsText("flink-staging/flink-streaming/flink-streaming-examples/src/main/resources/evaluation");
				//.setParallelism(1);
	}


	/**
	 * Creates a DataStreamSource of GaussianDistribution items out of the params at input.
	 *
	 * @param env the StreamExecutionEnvironment.
	 * @return the DataStreamSource
	 */
	public static DataStreamSource<GaussianDistribution> createSource(StreamExecutionEnvironment env, final Properties props) {
		return env.addSource(new GaussianStreamGenerator(props));
	}

	/********* debug functions ***********/
	public static DataStreamSource<Integer> createOrderedSrc(StreamExecutionEnvironment env) {
		return env.addSource(new RichSourceFunction<Integer>() {
			int counter = 0;
			@Override
			public void run(Collector<Integer> collector) throws Exception {
				while (counter < MAX_COUNT) {
					counter++;
					collector.collect(counter);
				}
			}
			@Override
			public void cancel() {

			}
		});
	}


}
