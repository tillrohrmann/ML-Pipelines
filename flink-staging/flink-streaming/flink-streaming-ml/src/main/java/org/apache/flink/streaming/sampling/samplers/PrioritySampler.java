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
import org.apache.flink.streaming.examples.sampling.helpers.SamplingUtils;
import org.apache.flink.streaming.examples.sampling.helpers.StreamTimestamp;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by marthavk on 2015-04-21.
 */
public class PrioritySampler<T> extends RichMapFunction<Tuple3<T, StreamTimestamp, Long>,
		ChainSample<Tuple3<T, StreamTimestamp, Long>>> implements Sampler<Tuple3<T,StreamTimestamp,Long>>{

	ChainSample<Tuple3<T, StreamTimestamp, Long>> chainSample;
	ArrayList<LinkedList<Double>> priorityList;
	Long windowSize;

	public PrioritySampler(int lSize, long lWindowSize) {
		this.chainSample = new ChainSample<Tuple3<T, StreamTimestamp, Long>>(lSize);
		this.windowSize = lWindowSize;
		this.priorityList = new ArrayList<LinkedList<Double>>();
		initializeLists();
	}

	/**  METHODS EXTENDING RichMapFunction ABSTRACT CLASS **/
	@Override
	public ChainSample<Tuple3<T, StreamTimestamp, Long>> map(Tuple3<T, StreamTimestamp, Long> value) throws Exception {

		sample(value);
		return this.chainSample;
	}

	/** METHODS IMPLEMENTING Sampler INTERFACE **/
	@Override
	public ArrayList<Tuple3<T,StreamTimestamp,Long>> getElements() {
		return null;
	}

	@Override
	public void sample(Tuple3<T, StreamTimestamp, Long> element) {
		//printIndexedString("\nEnters map function. Item:" + element.toString(),0);
		//printIndexedString("chainSample: " + chainSampletoString(chainSample),0);
		//printIndexedString("priotiryList: " + prioritiesToString(), 0);
		//update expired elements
		StreamTimestamp currentTimestamp = new StreamTimestamp();
		update(currentTimestamp);
		//printIndexedString(currentTimestamp.toString(),0);

		//printIndexedString("["+element.f2+"]: " + (currentTimestamp.getTimestamp() - element.f1.getTimestamp()),0);

		//printIndexedString("After Update:\nUpdated Sample:" + chainSampletoString(chainSample),0);
		//printIndexedString("Priorities List:" + prioritiesToString(),0);


		//assign k priorities between 0,1
		ArrayList<Double> priorities = assignPriorities();
		//printIndexedString("Assigned Priorities: " + assignedPriorsToString(priorities), 0);

		//place new sample
		placeInList(element, priorities);
		//printIndexedString("Item Placed in List:" + chainSampletoString(chainSample),0);
		//printIndexedString("New Priorities List:" + prioritiesToString(), 0);
	}

	@Override
	public int size() {
		return this.chainSample.getSize();
	}

	@Override
	public int maxSize() {
		return this.chainSample.getMaxSize();
	}

	/** PRIORITY SAMPLER METHODS **/
	public ArrayList<Double> assignPriorities (){
		ArrayList<Double> priorities = new ArrayList<Double>();
		for (int i=0; i<maxSize(); i++) {
			priorities.add(SamplingUtils.randomPriority());
		}
		return priorities;
	}

	/**
	 * initialize priorityList and chainSample with null elements
	 */
	public void initializeLists() {

		//initialize priority list
		for (int i=0; i<maxSize(); i++) {
			LinkedList<Double> priorityInitList = new LinkedList<Double>();
			priorityInitList.add(-1.0);
			priorityList.add(priorityInitList);
			chainSample.addSample(null);
		}

	}

	/**
	 * for each position in the sample checks if the assigned priority
	 * of the new item for this position is bigger than any priority in
	 * the list. If so, deletes all elements with lower priority and
	 * chains the current element in the chainSample and its priority
	 * in the priorityList
	 * @param item
	 * @param priorities
	 */
	public void placeInList(Tuple3<T, StreamTimestamp, Long> item, ArrayList<Double> priorities) {

		//printIndexedString("\t***placeInList",0);
		for (int pos=0; pos<maxSize(); pos++) {

			Double p = priorities.get(pos);
			int lastElement = priorityList.get(pos).size()-1;

			for (int i=0; i<priorityList.get(pos).size(); i++) {
				double currentP = priorityList.get(pos).get(i);
				//printIndexedString("\tpos ="+pos+"  last element = " + lastElement + "  currentP = " + currentP
				//		+ "  i = " + i,0);
				if (p>currentP) {

				//	printIndexedString("\tp>currentP",0);
					//delete all priorities in the queue and the sample
					priorityList.get(pos).subList(i, priorityList.get(pos).size()).clear();
					chainSample.get(pos).subList(i, chainSample.get(pos).size()).clear();

					//add this item to the queue and its priority to the sample
					priorityList.get(pos).add(p);
					chainSample.get(pos).add(item);
				//	printIndexedString("\tpriorityList: " + prioritiesToString(),0);
				//	printIndexedString("\tchainSample: " + chainSampletoString(chainSample),0);
					break;
				}
				else if (i == lastElement) {
				//	printIndexedString("\ti==lastElement",0);
					priorityList.get(pos).add(p);
					chainSample.get(pos).add(item);
				//	printIndexedString("\tpriorityList: " + prioritiesToString(),0);
				//	printIndexedString("\tchainSample: " + chainSampletoString(chainSample),0);
				}
				else {
					//do nothing
				}
			}
		}


	}

	/**
	 * checks if the timestamp of all sampled elements is between the
	 * declared window. If not, pops them out of the list
	 */
	public void update(StreamTimestamp timestamp) {
		//printIndexedString("\tenters update, timestamp = " + timestamp.getTimestamp(),0);

		//TODO : FIX
		for (int i=0; i<chainSample.getMaxSize(); i++) {
			//printIndexedString("\t***i=" + i, 0);
			LinkedList<Tuple3<T, StreamTimestamp, Long>> listInPos = chainSample.get(i);

			if (!listInPos.contains(null)) {
				//printIndexedString("\tlistInPos does not contain null",0);
				int counter = 0;
				//printIndexedString("\tcounter="+counter,0);
				Iterator<Tuple3<T, StreamTimestamp, Long>> iter = listInPos.iterator();
				while (iter.hasNext()) {
					Tuple3<T,StreamTimestamp,Long> nextItem = iter.next();
					//printIndexedString("\tnextItem = " + nextItem.toString(),0);
					//printIndexedString("\twindow = " + windowSize,0);
					//printIndexedString("\ttimestamp.getTimestamp(null) = "+timestamp.getTimestamp(null),0);
					//printIndexedString("\tnextItem.f1.getTimestamp(null) = "+nextItem.f1.getTimestamp(null),0);
					//printIndexedString("\tdiafora = " +(timestamp.getTimestamp() - nextItem.f1.getTimestamp()),0);
					if (timestamp.getTimestamp() - nextItem.f1.getTimestamp() > windowSize) {
						counter ++;
						//printIndexedString("\tcounter = "+counter,0);
					}
				}


				listInPos.subList(0, counter).clear();
				priorityList.get(i).subList(0, counter).clear();

				if (listInPos.isEmpty()) {
					//printIndexedString("\tlistInPos is empty",0);
					listInPos.add(null);
				}
				//printIndexedString("\t$$$$$prioritylist=" + priorityList + " " + priorityList.isEmpty(),0);
				if (priorityList.get(i).isEmpty()) {
					//printIndexedString("\tpriorityList is empty",0);
					priorityList.get(i).add(-1.0);
				}
			}
		}
	}

	/*** DEBUG MESSAGES
	 * @param chain***/
	public String chainSampletoString(ChainSample<Tuple3<T, StreamTimestamp, Long>> chain) {
		String chainSampleStr;
		chainSampleStr = "[";
		Iterator<LinkedList> iter= chain.iterator();
		while (iter.hasNext()) {
			LinkedList<Tuple3<Object, Timestamp, Long>> list = iter.next();
			chainSampleStr += "(";
			if (!list.contains(null)) {
				for (int i = 0; i < list.size(); i++) {
					chainSampleStr += list.get(i).f2 + "->";
				}
			}
			chainSampleStr +=")";
		}
		chainSampleStr += "]";
		return chainSampleStr;
	}

	String prioritiesToString() {
		String prStr;
		prStr = "[";

		Iterator<LinkedList<Double>> iter = priorityList.iterator();
		while (iter.hasNext()) {
			LinkedList<Double> list = iter.next();
			prStr += "(";
			if (!list.contains(null)) {
				for (int i = 0; i < list.size(); i++) {
					prStr += list.get(i).floatValue() + "->";
				}
			}
			prStr +=")";
		}
		prStr += "]";
		return prStr;
	}

	String assignedPriorsToString(ArrayList<Double> p) {
		String pStr = "[";
		for (int i=0; i<p.size(); i++) {
			pStr += " " + p.get(i).floatValue() + " ";
		}
		pStr += "]";
		return pStr;
	}

	void printIndexedString(String str, int subtaskIndex) {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		if (context.getIndexOfThisSubtask() == subtaskIndex) {
			System.out.println(str);
		}
	}

}

