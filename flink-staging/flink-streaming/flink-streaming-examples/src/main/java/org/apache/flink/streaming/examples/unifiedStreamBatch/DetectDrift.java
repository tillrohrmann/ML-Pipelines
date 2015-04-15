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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.StreamOperator;

import java.io.Serializable;

public class DetectDrift extends StreamOperator<Tuple2<Double, Integer>,Tuple2<Double, Integer>> {

	private final TriggerBatchJobFunction userFun;


	public DetectDrift(BatchJob batchJob){
		this (new TriggerBatchJobFunction(batchJob));
	}

	public DetectDrift(TriggerBatchJobFunction userFunction) {
		super(userFunction);
		userFun = userFunction;
	}

	@Override
	public void run() throws Exception {
		while (readNext()!=null){
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {
		if(condition()){
			collector.collect(new Tuple2<Double, Integer>(nextObject.f0,nextObject.f1+1));
		}else{
			userFun.triggerBatchJob();
			collector.collect(new Tuple2<Double, Integer>(nextObject.f0,nextObject.f1));
		}
	}

	private boolean condition() {
		if (nextObject.f0!=0.0){
			return true;
		}
		else{
			return false;
		}
	}

	static class TriggerBatchJobFunction implements Function, Serializable{
		private final BatchJob jobToTrigger ;

		TriggerBatchJobFunction(BatchJob batchJob) {
			this.jobToTrigger = batchJob;
		}

		public void triggerBatchJob(){
			new Thread(jobToTrigger).start();
		}
	}
}
