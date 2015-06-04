/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.examples.unifiedStreamBatch;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


public class LambdaTriggeredJoin {

	private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(LambdaPeriodicJoin.class);

	private static final String JARDependencies = "/Users/fobeligi/workspace/flink/flink-staging/" +
			"flink-streaming/flink-streaming-examples/target/flink-streaming-examples-0.9-SNAPSHOT-LambdaTriggeredJoin.jar";

	// Trigger batch job to run upon condition and streamJob to run continuously
	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

	static BatchJob periodicBatchJob;

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment batchEnvironment = ExecutionEnvironment.createRemoteEnvironment("127.0.0.1",
				6123, 1, JARDependencies);

		final StreamExecutionEnvironment streamEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1",
				6123, 1, JARDependencies);

		CsvReader csvR = batchEnvironment.readCsvFile("/Users/fobeligi/workspace/master-thesis/dataSets/UnifiedBatchStream.csv");
		csvR.lineDelimiter("\n");
		DataSet<Tuple2<Double, Integer>> batchDataSet = csvR.types(Double.class, Integer.class);

		batchDataSet.write(new TypeSerializerOutputFormat<Tuple2<Double, Integer>>(),
				"/Users/fobeligi/workspace/master-thesis/dataSets/FlinkTmp/temp",
				FileSystem.WriteMode.OVERWRITE);

		DataStream<String> dataSetStream = streamEnvironment.readFileStream(
				"file:///Users/fobeligi/workspace/master-thesis/dataSets/temp", 1000,
				FileMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED);

		SingleOutputStreamOperator ds = dataSetStream.map(new MapFunction<String, Tuple2<Double, Integer>>() {
			@Override
			public Tuple2<Double, Integer> map(String line) throws Exception {
				String[] temp = line.split(",");
				return new Tuple2(Double.valueOf(temp[0]), Integer.valueOf(temp[1]));
			}
		});
		ds.print();

		periodicBatchJob = new BatchJob(batchEnvironment);
//		DetectDrift detectDrift = new DetectDrift(periodicBatchJob);
//		ds.transform("driftDetection", ds.getType(), detectDrift);
//
		DataStream d = ds.flatMap(new FlatMapFunction<Tuple2<Double,Integer>,Tuple2<Double,Integer>>() {

			@Override
			public void flatMap(Tuple2<Double,Integer> value, Collector<Tuple2<Double, Integer>> out) throws Exception {
				if (value.f1 == 0.0) {
					new Thread(periodicBatchJob).start();
				} else {
					out.collect(new Tuple2<Double, Integer>(value.f0, value.f1 + 1));
				}
			}
		});
		d.print();

		streamEnvironment.execute();
	}

}
