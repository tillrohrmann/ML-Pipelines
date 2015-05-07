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


package org.apache.flink.streaming.sampling.generators;

import org.apache.commons.math3.distribution.NormalDistribution;

import java.io.Serializable;
import java.util.Random;

*
 * Created by marthavk on 2015-03-18.



public class GaussianDistribution extends NormalDistribution implements Serializable, NumberGenerator {

*
	 * Constructs an empty distribution


	public GaussianDistribution() {

	}

*
	 * Constructs a Normal Distribution
	 * @param cmeans the means of the Normal Distribution
	 * @param cstdev the standard deviation of the Normal Distribution


	public GaussianDistribution(double cmeans, double cstdev) {
		super(cmeans, cstdev);
	}


* AUXILIARY METHODS *

	@Override
	public String toString() {
		return "[" + this.getMean() + ", " + this.getStandardDeviation() + "]";
	}


	@Override
	public double generate() {
		return (new Random().nextGaussian() * this.getStandardDeviation() + this.getMean());
	}
}
*/
