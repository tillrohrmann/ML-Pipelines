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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.streaming.sampling.helpers.GaussianDistribution;
import org.apache.flink.streaming.sampling.generators.GaussianStreamGenerator;
import org.apache.flink.streaming.sampling.helpers.DriftDetector;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;

import java.util.Properties;

/**
 * Created by marthavk on 2015-05-11.
 */
public class DriftDetectionExample<T> {

	public static long MAX_COUNT;  // max count of generated numbers
	public static int SAMPLE_SIZE;
	public static Properties initProps = new Properties();

	// *************************************************************************
	// PROGRAM
	// *************************************************************************
	public static void main(String[] args) throws Exception {

		/*read properties file and set static variables*/
		initProps = SamplingUtils.readProperties(SamplingUtils.path + "distributionconfig.properties");
		MAX_COUNT = Long.parseLong(initProps.getProperty("maxCount"));
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
	 *
	 * @param env
	 * @param initProps
	 */
	public static void evaluateSampling(StreamExecutionEnvironment env, final Properties initProps) {

		int sampleSize = SAMPLE_SIZE;

		/*create source*/
		DataStreamSource<GaussianDistribution> source = createSource(env, initProps);

		/*generate random numbers according to Distribution parameters*/
		SingleOutputStreamOperator<GaussianDistribution, ?> shuffledSource = source.shuffle();

		shuffledSource.map(new MapFunction<GaussianDistribution, Tuple2<GaussianDistribution, Double>>() {
			@Override
			public Tuple2<GaussianDistribution, Double> map(GaussianDistribution value) throws Exception {
				return new Tuple2<GaussianDistribution, Double>(value, value.generate());
			}
		})
				.map(new DriftDetector())
				.addSink(new RichSinkFunction<Tuple4<GaussianDistribution, Double, Long, Boolean>>() {
					@Override
					public void invoke(Tuple4<GaussianDistribution, Double, Long, Boolean> value) throws Exception {
						StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
						if (context.getIndexOfThisSubtask()==0 && value.f3) {

							System.out.println( "****** Change detection: " + value.toString());
						}
						else {
							//System.out.println(value.toString());
						}
					}
				});
				/*
				.addSink(new RichSinkFunction<Tuple3<GaussianDistribution, Double, Boolean>>() {

					@Override
					public void invoke(Tuple3<GaussianDistribution, Double, Boolean> value) throws Exception {
						StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
							if (context.getIndexOfThisSubtask()==0 && value.f2) {
								System.out.println(value.f0 + " -> " + value.f2);
							}
					}
				});*/
				/*.writeAsText(SamplingUtils.path + "drift");*/
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
}
