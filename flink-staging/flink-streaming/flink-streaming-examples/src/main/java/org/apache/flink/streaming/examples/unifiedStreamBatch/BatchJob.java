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

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.LocalExecutor;

import java.io.Serializable;


public class BatchJob implements Runnable, Serializable {

	private static Plan plan;
	private transient LocalExecutor executor;

	public BatchJob(ExecutionEnvironment execEnv) {
		this.plan = execEnv.createProgramPlan();
		this.executor = new LocalExecutor();
		executor.setTaskManagerNumSlots(2);
		try {
			executor.start();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public void run() {

		try {//start and stop in finally the executor
			System.out.println("-------------------------------Thread called!!!!---------------------------");
			executor.executePlan(plan);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}