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
package org.apache.flink.streaming.sampling.airlines;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;


/**
 * Created by marthavk on 2015-05-21.
 * <p>
 * Tuple2 has the fields: f0->Integer[] and f1->String[]
 * <p>
 * Integer[] is an array of 11 values containing in the following order:
 * [year, month, day of month, day of week, CRS depart time, CRS arrival time
 * flight number, actual elapsed time, distance, diverted, delay]
 * <p>
 * String[] is an array of 3 values containing in the following order:
 * [unique carrier, origin, destination]
 */

public class AirlinesExample {

	public static void main(String[] args) throws Exception {
		String path = SamplingUtils.path;
		/*set execution environment*/
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> source = env.readTextFile(path + "2008_14col.data");

		SingleOutputStreamOperator<Tuple2<Integer[], String[]>, ?> dataStream =
				source.map(new MapFunction<String, Tuple2<Integer[], String[]>>() {

					@Override
					public Tuple2<Integer[], String[]> map(String record) throws Exception {

						String[] values = record.split(",");
						Integer[] vInt = new Integer[11];
						int[] integerFields = new int[]{0, 1, 2, 3, 4, 5, 7, 8, 11, 12, 13};

						for (int i = 0; i < integerFields.length; i++) {
							int index = integerFields[i];
							vInt[i] = Integer.parseInt(values[index]);
						}

						String[] vStr = new String[3];
						int[] stringFields = new int[]{6, 9, 10};
						for (int i = 0; i < stringFields.length; i++) {
							int index = stringFields[i];
							vStr[i] = (values[index]);
						}

						return new Tuple2<Integer[], String[]>(vInt, vStr);

					}
				});

		dataStream.addSink(new RichSinkFunction<Tuple2<Integer[], String[]>>() {
			@Override
			public void invoke(Tuple2<Integer[], String[]> value) throws Exception {
				System.out.println(value.toString());
			}
		});

		/*get js for execution plan*/
		System.err.println(env.getExecutionPlan());

		/*execute program*/
		env.execute();
	}


}
