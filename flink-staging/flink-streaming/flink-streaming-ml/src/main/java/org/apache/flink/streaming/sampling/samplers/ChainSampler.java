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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.sampling.helpers.StreamTimestamp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by marthavk on 2015-04-07.
 */

public class ChainSampler<T> extends RichMapFunction<Tuple3<T, StreamTimestamp, Long>,
		ChainSample<Tuple3<T, StreamTimestamp, Long>>> implements Sampler<Tuple3<T, StreamTimestamp, Long>> {

	ChainSample<Tuple3<T, StreamTimestamp, Long>> chainSample;

	int windowSize;

	public ChainSampler(int lSize, int lWindowSize) {
		chainSample = new ChainSample<Tuple3<T, StreamTimestamp, Long>>(lSize);
		//TODO: add initialization to cTor of chainSample (also suitable for PrioritySampler)
		//initializeList();
		windowSize = lWindowSize;

	}

	@Override
	public ChainSample<Tuple3<T, StreamTimestamp, Long>> map(Tuple3<T, StreamTimestamp, Long> value) throws Exception {
		//TODO: fix bug in distributed sampling. When shuffling the stream it is impossible to know which indices will belong in the same instance
		//printIndexedString("\nEnters map. Item : " + value.toString(),0);
		//printIndexedString("beginning of map : " + chainSampletoString(chainSample),0);
		storeChainedItems(value);
		//printIndexedString("after storing chained items : " + chainSampletoString(chainSample),0);
		updateExpiredItems(value);
		//printIndexedString("after updating expired items : " + chainSampletoString(chainSample),0);
		sample(value);
		//printIndexedString("after sampling value : " + chainSampletoString(chainSample),0);

		return chainSample;

	}

	@Override
	public ArrayList<Tuple3<T, StreamTimestamp, Long>> getElements() {
		return chainSample.extractSample();
	}

	@Override
	public void sample(Tuple3<T, StreamTimestamp, Long> item) {
		if (!chainSample.isFull()) {
			int pos = chainSample.getSize();
			chainSample.addSample(item);

			long futureReplacement = selectReplacement(item);
			Tuple3<T, StreamTimestamp, Long> futureItem
					= new Tuple3<T, StreamTimestamp, Long>(null, null, futureReplacement);
			chainSample.chainItem(futureItem, pos);

		} else {
			double prob = (double) chainSample.getMaxSize() / SamplingUtils.max(chainSample.getMaxSize(), item.f2);
			if (SamplingUtils.flip(prob)) {

				int pos = SamplingUtils.randomBoundedInteger(0, chainSample.getSize() - 1);
				chainSample.replaceChain(pos, item);

				long futureReplacement = selectReplacement(item);
				Tuple3<T, StreamTimestamp, Long> futureItem
						= new Tuple3<T, StreamTimestamp, Long>(null, null, futureReplacement);

				chainSample.chainItem(futureItem, pos);
			}
		}
	}

	@Override
	public int size() {
		return chainSample.getSize();
	}

	@Override
	public int maxSize() {
		return chainSample.getMaxSize();
	}


	/** CHAIN SAMPLING METHODS **/

	/**
	 * @return the index for replacement when current item expires
	 */
	public long selectReplacement(Tuple3<T, StreamTimestamp, Long> item) {
		return SamplingUtils.randomBoundedLong(item.f2 + 1, item.f2 + windowSize);
	}

	/**
	 * Checks if the index of the current item has been selected in the past
	 * if so, it chains the item and updates all structures accordingly
	 *
	 * @param item
	 */
	void storeChainedItems(Tuple3<T, StreamTimestamp, Long> item) {
		for (int i = 0; i < chainSample.getSize(); i++) {
			LinkedList<Tuple3<T, StreamTimestamp, Long>> currentList = chainSample.get(i);
			if (currentList.getLast().f2.equals(item.f2)) {
				//System.out.print(" " + item.f2);
				currentList.removeLast();
				chainSample.chainItem(item, i);

				long replacement = selectReplacement(item);
				Tuple3<T, StreamTimestamp, Long> indicator = new Tuple3<T, StreamTimestamp, Long>(null, null, replacement);
				chainSample.chainItem(indicator, i);
			}
		}
	}

	/**
	 * updates all expired Items (pops the heads of the chains so that
	 * the chained elements are now in the sample)
	 */
	void updateExpiredItems(Tuple3<T, StreamTimestamp, Long> item) {

		int threshold = (int) (item.f2 - windowSize);
		for (int pos = 0; pos < chainSample.getSize(); pos++) {
			if (chainSample.get(pos).peek().f2 <= threshold) {
				chainSample.get(pos).pollFirst();
			}
		}
	}

	void printIndexedString(String str, int subtaskIndex) {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		if (context.getIndexOfThisSubtask() == subtaskIndex) {
			System.out.println(str);
		}
	}

	/**
	 * initialize priorityList and chainSample with null elements
	 */
	public void initializeList() {

		//initialize chainSample with null elements
		for (int i = 0; i < maxSize(); i++) {
			chainSample.addSample(null);
		}

	}

	public String chainSampletoString(ChainSample<Tuple3<T, StreamTimestamp, Long>> chain) {
		String chainSampleStr;
		chainSampleStr = "[";
		Iterator<LinkedList> iter = chain.iterator();
		while (iter.hasNext()) {
			LinkedList<Tuple3<Object, Timestamp, Long>> list = iter.next();
			chainSampleStr += "(";
			if (!list.contains(null)) {
				for (int i = 0; i < list.size(); i++) {
					chainSampleStr += list.get(i).f2 + "->";
				}
			}
			chainSampleStr += ")";
		}
		chainSampleStr += "]";
		return chainSampleStr;
	}

}
