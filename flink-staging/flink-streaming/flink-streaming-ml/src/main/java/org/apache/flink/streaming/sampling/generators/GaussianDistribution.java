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
package org.apache.flink.streaming.sampling.generators;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.flink.streaming.sampling.samplers.Sample;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by marthavk on 2015-03-18.
 */

public class GaussianDistribution implements Serializable, NumberGenerator {

	double mean;
	double sigma;

	/**
	 * Constructs an empty distribution
	 */
	public GaussianDistribution() {

	}

	/**
	 * Constructs a Normal Distribution
	 * @param cmeans the means of the Normal Distribution
	 * @param cstdev the standard deviation of the Normal Distribution
	 */
	public GaussianDistribution(double cmeans, double cstdev) {
		this.mean = cmeans;
		this.sigma = cstdev;
	}

	/**
	 * Constructs an empirical normal distribution
	 * @param sample the sampled values
	 */
	public GaussianDistribution(Sample<Double> sample) {

		SummaryStatistics stats = new SummaryStatistics();
		for (Double value : sample.getSample()) {
			stats.addValue(value);
		}

		this.mean = stats.getMean();
		this.sigma = stats.getStandardDeviation();
	}


	/** GETTERS **/
	public double getMean() {
		return mean;
	}


	public double getStandardDeviation() {
		return sigma;
	}


	/** UPDATES **/
	/*public void updateMean(long count, double mStep, int interval) {
		this.mean += (count % interval == 0 ? mStep : 0);
	}

	public void updateSigma(long count, double sStep, int interval) {
		this.sigma += (count % interval == 0 ? sStep : 0);
	}*/

	public void update(double newMean, double newStDev) {
		this.mean = newMean;
		this.sigma = newStDev;
	}

	public double density(double x) {
		double a = 1/(sigma*Math.sqrt(2*Math.PI));
		double b = Math.exp(-Math.pow((x-mean),2)/(2*Math.pow(sigma,2)));
		return a*b;
	}


	/** AUXILIARY METHODS **/
	public String toString() {
		return "[" + this.mean + ", " + this.sigma + "]";
	}


	@Override
	public double generate() {
		return (new Random().nextGaussian() * sigma + mean);
	}
}
