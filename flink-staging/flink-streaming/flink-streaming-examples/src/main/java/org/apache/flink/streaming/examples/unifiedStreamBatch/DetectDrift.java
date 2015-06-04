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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import java.io.Serializable;


public class DetectDrift
		extends AbstractUdfStreamOperator<Tuple2<Double, Integer>, TriggerBatchJobFunction>
		implements OneInputStreamOperator<Tuple2<Double, Integer>, Tuple2<Double, Integer>>, Serializable {

	private final TriggerBatchJobFunction userFun;


	public DetectDrift(BatchJob batchJob) {
		this(new TriggerBatchJobFunction(batchJob));
	}

	public DetectDrift(TriggerBatchJobFunction userFunction) {
		super(userFunction);
		userFun = userFunction;
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void processElement(Tuple2<Double, Integer> element) throws Exception {
		if (element.f1 == 0){
			userFun.triggerBatchJob();
		}
		output.collect(element);
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
	}

	@Override
	public void close() throws Exception {
		super.close();
	}
}
