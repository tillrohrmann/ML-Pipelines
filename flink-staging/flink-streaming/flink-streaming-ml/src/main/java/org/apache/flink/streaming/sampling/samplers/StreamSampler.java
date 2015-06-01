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
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

/**
 * Created by marthavk on 2015-06-01.
 */
public class StreamSampler<IN> extends AbstractUdfStreamOperator<IN, SampleFunction<IN>>
		implements OneInputStreamOperator<IN, IN> {


	public StreamSampler(SampleFunction<IN> userFunction) {
		super(userFunction);

	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		FunctionUtils.setFunctionRuntimeContext(userFunction, runtimeContext);
		FunctionUtils.openFunction(userFunction, parameters);
		//logic for the thread
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				while(true){
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					output.collect(userFunction.getRandomEvent());
				}


			}
		});
		output.collect(userFunction.getRandomEvent());
	}


	@Override
	public void processElement(IN element) throws Exception {
		userFunction.sample(element);
	}
}
