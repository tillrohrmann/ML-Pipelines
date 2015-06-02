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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by marthavk on 2015-04-07.
 */
public class Chain<T,S> extends Buffer<LinkedList<Tuple2<T,S>>> implements Serializable, Iterable {

	public Chain(int size) {
		sample = new ArrayList<LinkedList<Tuple2<T,S>>>();
		maxSize = size;
	}

	@Override
	public void setMaxSize(int s) {
		this.maxSize = s;
	}

	@Override
	public int getMaxSize() {
		return maxSize;
	}

	public void addSample(Tuple2<T,S> item) {
		LinkedList<Tuple2<T,S>> newList = new LinkedList<Tuple2<T,S>>();
		newList.add(item);
		sample.add(newList);
	}

	@Override
	public int getSize() {
		return sample.size();
	}

	public void reset() {
		sample.clear();
	}

	public ArrayList<T> extractSample() {
		ArrayList<T> output = new ArrayList<T>();
		Iterator<LinkedList<Tuple2<T,S>>> it = sample.iterator();
		while (it.hasNext()) {
			output.add(it.next().peekFirst().f0);
		}
		return output;
	}

	public LinkedList<Tuple2<T,S>> get(int i) {
		return sample.get(i);
	}

	@Override
	boolean isFull() {
		return sample.size() == maxSize;
	}

	@Override
	public Iterator iterator() {
		return sample.iterator();
	}

	@Override
	public String toString() {
		return extractSample().toString();
	}



	public void replaceChain(int pos, Tuple2<T,S> item) {
		LinkedList<Tuple2<T,S>> newList = new LinkedList<Tuple2<T,S>>();
		newList.add(item);
		super.replaceSample(pos, newList);
	}

	/**
	 * Appends item in the LinkedList of chainSample located in position pos
	 *
	 * @param item
	 * @param pos
	 */
	public void chainItem(Tuple2<T,S> item, int pos) {
		this.get(pos).add(item);
	}
}
