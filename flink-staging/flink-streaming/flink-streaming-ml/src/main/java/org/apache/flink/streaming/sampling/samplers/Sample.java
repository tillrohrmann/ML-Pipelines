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

import org.apache.flink.streaming.sampling.helpers.SamplingUtils;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by marthavk on 2015-03-26.
 */
public class Sample<T> implements Serializable {

	int maxSize;
	ArrayList<T> sample;

	public Sample() {
		sample = new ArrayList<T>();
	}

	public Sample(int lMaxSize) {
		sample = new ArrayList<T>();
		maxSize = lMaxSize;
	}

	public T generate() {
		if (!sample.isEmpty()) {
			int pos = SamplingUtils.nextRandInt(sample.size());
			return sample.get(pos);
		}
		return null;
	}

	void setMaxSize(int s) {
		this.maxSize = s;
	}

	int getMaxSize() {
		return maxSize;
	}

	public void addSample(T item) {
		sample.add(item);
	}

	void replaceSample(int pos, T item) {
		sample.set(pos, item);
	}

	void replaceAtRandom(T item) {
		// choose position uniformly at random
		int pos = SamplingUtils.nextRandInt(sample.size());
		// replace element at pos
		this.replaceSample(pos, item);
	}

	void removeSample(int pos) {
		sample.remove(pos);
	}

	int getSize() {
		return sample.size();
	}

	public ArrayList<T> getSample() {
		return sample;
	}

	public double[] getSampleAsArray() {
		double[] array = new double[sample.size()];
		for (int i = 0; i < sample.size(); i++) {
			array[i] = (Double) sample.get(i);
		}
		return array;
	}

	boolean isFull() {
		return sample.size() == maxSize;
	}

	@Override
	public String toString() {
		return sample.toString();
	}

	public void discard(double proportion) {
		int tuplesToEvict = (int) Math.floor(this.getSize() * proportion);
		for (int i = 0; i < tuplesToEvict; i++) {
			int pos = SamplingUtils.nextRandInt(this.getSize());
			this.removeSample(pos);
		}
	}


}
