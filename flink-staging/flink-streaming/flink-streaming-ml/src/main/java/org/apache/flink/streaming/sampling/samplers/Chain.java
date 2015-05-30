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

/**
 * Created by marthavk on 2015-04-07.
 */
public class Chain<T> extends Sample implements Serializable, Iterable {


	public Chain(int size) {
		sample = new ArrayList<LinkedList<T>>();
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

	@Override
	public void addSample(Object item) {
		LinkedList<T> newList = new LinkedList<T>();
		newList.add((T) item);
		sample.add(newList);
	}

	@Override
	public int getSize() {
		return sample.size();
	}


	public ArrayList<T> extractSample() {
		ArrayList<T> output = new ArrayList<T>();
		Iterator<LinkedList<T>> it = sample.iterator();
		while (it.hasNext()) {
			output.add(it.next().peekFirst());
		}
		return output;
	}

	public LinkedList<T> get(int i) {
		return (LinkedList<T>) sample.get(i);
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


	public String toRichString() {
		String str = "";

		Iterator<LinkedList<Long>> it = sample.iterator();
		while (it.hasNext()) {
			LinkedList<Long> crList = it.next();
			str += "\n" + crList.toString();
		}
		return str;
	}

	public void replaceChain(int pos, T item) {
		LinkedList<T> newList = new LinkedList<T>();
		newList.add(item);
		super.replaceSample(pos, newList);
	}

	/**
	 * Appends item in the LinkedList of chainSample located in position pos
	 *
	 * @param item
	 * @param pos
	 */
	public void chainItem(T item, int pos) {
		this.get(pos).add(item);
	}
}
