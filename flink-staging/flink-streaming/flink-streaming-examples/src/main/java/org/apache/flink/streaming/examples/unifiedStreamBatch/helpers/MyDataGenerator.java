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
package org.apache.flink.streaming.examples.unifiedStreamBatch.helpers;


import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.flink.api.java.tuple.Tuple2;
import scala.Tuple1;

import java.util.ArrayList;
import java.util.List;


public class MyDataGenerator implements IDataPatternFunction {

	private static List<Double> x_dataPoints;
	private static List<Double> data;
	private double withError;
	private NormalDistribution myErrorDistribution;

	public MyDataGenerator(double error) {
		this.withError = error;
		if (withError != 0.0) {
			myErrorDistribution = new NormalDistribution(0.0, withError);
		}
	}

	@Override
	public List patternFunction() {

		List<Integer> y = new ArrayList<Integer>();

		int w1 = 1;
		int w2 = 2;
		double point;

		//pattern function: y = w1*sin(x) + w2*x + error
		if (withError != 0.0) {
			for (int i = 0; i < x_dataPoints.size(); i++) {
				point = w1 * Math.sin(x_dataPoints.get(i)) + w2 * x_dataPoints.get(i) + myErrorDistribution.sample();
				//if no classification is needed just add the y value of the dataPoint in the list
				y.add(classifyPoint(point, x_dataPoints.get(i), w2));
			}
		} else {
			for (int i = 0; i < x_dataPoints.size(); i++) {
				point = w1 * Math.sin(x_dataPoints.get(i)) + w2 * x_dataPoints.get(i);
				//if no classification is needed just add the y value of the dataPoint in the list
				y.add(classifyPoint(point, x_dataPoints.get(i), w2));
			}
		}
		return y;
	}

	private Integer classifyPoint(double point, Double x_temp, int w2) {
		if (point >= w2 * x_temp) {
			return 1;
		} else {
			return (-1);
		}
	}

	public static void main(String[] args) {

		MyDataGenerator mdpg = new MyDataGenerator(0.1);
		SyntheticDataGenerator sdg = new SyntheticDataGenerator(mdpg);

		x_dataPoints = sdg.generateData();
		System.out.println(x_dataPoints);
		data = sdg.labelData();
		System.out.println(data);
		List ds = new ArrayList();
		for (int i = 0; i < data.size(); i++) {
			ds.add(new Tuple2(x_dataPoints.get(i), data.get(i)));
		}
		Utils.writeCSV(ds, "/Users/fobeligi/Documents/dataSets/dataWithDrift/dataPoints.csv");
	}

}
