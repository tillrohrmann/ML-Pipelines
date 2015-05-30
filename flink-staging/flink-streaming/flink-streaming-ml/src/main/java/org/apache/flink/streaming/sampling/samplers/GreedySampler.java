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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.incrementalML.inspector.PageHinkleyTest;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.sampling.helpers.StreamTimestamp;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by marthavk on 2015-05-11.
 * Greedy sampler uses a change detection component in order to detect concept drift
 * in the stream. If such a change is detected then a certain percentage of tuples
 * from the reservoir are evicted and new ones are sampled using the biased reservoir sampling
 * algorithm.
 */
public class GreedySampler<IN> implements FlatMapFunction<IN, IN>, Sampler<IN> {

	Sample sample;

	/* Properties for Page Hinkley Test */
	PageHinkleyTest detector;
	double lambda, delta;

	/* Properties for Sampler */
	double evictionRate = 0.9;

	private boolean hasDrift = false;
	int count = 0;

	public GreedySampler(int size) {
		sample = new Sample(size);
		Properties props = SamplingUtils.readProperties(SamplingUtils.path + "distributionconfig.properties");
		lambda = Double.parseDouble(props.getProperty("lambda"));
		delta = Double.parseDouble(props.getProperty("delta"));
		detector = new PageHinkleyTest(lambda, delta, 30);
	}

	//TODO implement collector policy
	@Override
	public void flatMap(IN value, Collector<IN> out) throws Exception {
		count ++;
		sample(value);

	}

	@Override
	public ArrayList<IN> getElements() {
		return sample.getSample();
	}

	@Override
	public void sample(IN element) {
		Tuple3 inValue = (Tuple3) element;

		//StreamTimestamp changeTimeStamp = new StreamTimestamp();
		//System.out.println(changeTimeStamp.getTimestamp());

		/* define sampling policy according to drift*/
		detector.input(((Double) inValue.f0));
		hasDrift = detector.isChangedDetected();
		if (hasDrift) {
			hasDrift = false;
			detector.reset();
			sample.discard(evictionRate);
		}

		uniformSample(element);

	}


	public void uniformSample(IN element) {
		if (SamplingUtils.flip(count / sample.getMaxSize())) {
			if (!sample.isFull()) {
				sample.addSample(element);
			}
			else {
				sample.replaceAtRandom(element);
			}
		}
	}

	public void fifoSample(IN element) {
		if (sample.getSize() < sample.getMaxSize()) {
			sample.addSample(element);
		} else {
			sample.removeSample(0);
			sample.addSample(element);
		}
	}



}
