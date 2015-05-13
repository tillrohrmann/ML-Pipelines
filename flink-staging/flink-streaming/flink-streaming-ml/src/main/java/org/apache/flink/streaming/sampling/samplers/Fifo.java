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

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by marthavk on 2015-03-31.
 */
public class Fifo<T> extends Sample<T> implements Serializable, Iterable {

	public Fifo(int size) {
		super(size);
	}

	/**
	 * Adds a sample to the queue.
	 * If the queue is full evict the oldest (first) element
	 * then add the sample.
	 *
	 * @param item the sample to be added to the queue
	 */
	@Override
	public void addSample(T item) {
		if (getSize() < getMaxSize()) {
			sample.add(item);
		} else {
			sample.remove(0);
			sample.add(item);
		}
	}

	@Override
	public Iterator iterator() {
		return sample.iterator();
	}
}
