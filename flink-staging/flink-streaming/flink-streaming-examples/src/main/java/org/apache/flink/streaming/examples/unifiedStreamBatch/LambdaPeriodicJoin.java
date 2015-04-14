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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.FileMonitoringFunction;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class LambdaPeriodicJoin {

	private static final Logger log = Logger.getLogger(LambdaPeriodicJoin.class);

	private static final String JARDependencies = "/home/fobeligi/workspace/incubator-flink/flink-staging/" +
			"flink-streaming/flink-streaming-examples/target/flink-streaming-examples-0.9-SNAPSHOT-LambdaPeriodicJoin.jar";

	// schedule batch job to run periodically and streamJob to run continuously
	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);


	public static void main(String[] args) throws Exception {

		ExecutionEnvironment batchEnvironment = ExecutionEnvironment.createRemoteEnvironment("127.0.0.1",
				6123, 1, JARDependencies);

		StreamExecutionEnvironment streamEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1",
				6123, 1, JARDependencies);

		CsvReader csvR = batchEnvironment.readCsvFile("/home/fobeligi/dataSet-files/exampleCSV_1.csv");
		csvR.lineDelimiter("\n");
		DataSet<Tuple2<Double, Integer>> batchDataSet = csvR.types(Double.class, Integer.class);
		
		batchDataSet.write(new TypeSerializerOutputFormat<Tuple2<Double, Integer>>(), "/home/fobeligi/FlinkTmp/temp",
				FileSystem.WriteMode.OVERWRITE);

		batchDataSet.print();

		DataStream<Tuple2<String, Tuple2<Double, Integer>>> dataSetStream = streamEnvironment.readFileStream(
				"file:///home/fobeligi/FlinkTmp/temp", batchDataSet.getType(), 1000,
				FileMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED);

		dataSetStream.print();

		BatchJob periodicBatchJob = new BatchJob(batchEnvironment);
		final ScheduledFuture batchHandler = scheduler.scheduleWithFixedDelay(periodicBatchJob, 0, 5000, TimeUnit.MILLISECONDS);

		StreamingJob streamingJob = new StreamingJob(streamEnvironment);
		final ScheduledFuture streamHandler = scheduler.schedule(streamingJob, 1000, TimeUnit.MILLISECONDS);
//		run batch job periodically for an hour
		scheduler.schedule(new Runnable() {
			@Override
			public void run() {
				batchHandler.cancel(true);
			}
		}, 60, TimeUnit.MINUTES);

	}
}
