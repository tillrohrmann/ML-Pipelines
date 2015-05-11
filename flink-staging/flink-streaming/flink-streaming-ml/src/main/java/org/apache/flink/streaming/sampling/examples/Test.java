package org.apache.flink.streaming.sampling.examples;/*
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

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 * Created by marthavk on 2015-05-08.
 */
public class Test {

	public static void main(String args[]) {
		double[] a = {2,5,6,9,12};
		Percentile p = new Percentile();
		p.setData(a);

		/*System.out.println("For the data: " + a.toString() + " we have the following five-number summary:");
		System.out.println("Minimum: " + p.evaluate(0.1));
		System.out.println("First quartile: " + p.evaluate(25));
		System.out.println("Median: " + p.evaluate(50));
		System.out.println("Third quartile: " + p.evaluate(75));
		System.out.println("Maximum: " + p.evaluate(100));*/

		double quartile1 =  p.evaluate(25);
		double quartile3 = p.evaluate(75);
		double iqr = quartile3-quartile1;

//		System.out.println("\nIQR = 3rd quartile - 1st quartile = " + iqr);

//		System.out.println("\nTo determine if there are outliers we must consider the numbers that are 1.5·IQR or 10.5 beyond the quartiles.");

//		System.out.println("\nfirst quartile – 1.5·IQR = " + quartile1 + "-" + (1.5*iqr) + " = " + (quartile1-1.5*iqr));
//		System.out.println("third quartile + 1.5·IQR = "  + quartile3 + "+" + (1.5*iqr) + " = " + (quartile3+1.5*iqr));




		double iq = p.evaluate(75) - p.evaluate(25);
//		System.out.println(iq*1.5);

		double mean = 0;
		double sigma = 1;
		NormalDistribution dist = new NormalDistribution(mean, sigma);
		System.out.println(dist.inverseCumulativeProbability(0.25));

	}
}
