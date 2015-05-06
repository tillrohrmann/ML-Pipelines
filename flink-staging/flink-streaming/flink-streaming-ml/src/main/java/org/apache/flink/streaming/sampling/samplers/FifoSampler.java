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

import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;

/**
 * Created by marthavk on 2015-03-31.
 */
public class FifoSampler<IN> implements MapFunction<IN, Sample<IN>>, Sampler<IN> {

	Fifo fifoSample;

	public FifoSampler(int maxSize) {
		fifoSample = new Fifo(maxSize);
	}

	@Override
	public Sample<IN> map(IN value) throws Exception {
		sample(value);
		return fifoSample;
	}

	@Override
	public ArrayList<IN> getElements() {
		return fifoSample.getSample();
	}

	@Override
	public void sample(IN element) {
		fifoSample.addSample(element);
	}

	@Override
	public int size() {
		return fifoSample.getSize();
	}

	@Override
	public int maxSize() {
		return fifoSample.getMaxSize();
	}
}
