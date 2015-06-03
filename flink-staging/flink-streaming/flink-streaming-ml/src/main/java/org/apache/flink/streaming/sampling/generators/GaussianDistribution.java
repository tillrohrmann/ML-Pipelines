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

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.flink.streaming.sampling.helpers.SamplingUtils;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by marthavk on 2015-05-07.
 */
public class GaussianDistribution implements Serializable, NumberGenerator<Double> {
	Random n = new Random();
	double mean;
	double sigma;
	double outlierRate;

	public GaussianDistribution() {
		super();
	}

	public GaussianDistribution(double mean, double sigma) {
		this.mean = mean;
		this.sigma = sigma;
		this.outlierRate = 0;
	}

	public GaussianDistribution(double mean, double sigma, double outlierRate) {
		this.mean = mean;
		this.sigma = sigma;
		this.outlierRate = outlierRate;
	}

	public double getMean() {
		return this.mean;
	}

	public double getStandardDeviation() {
		return this.sigma;
	}

	@Override
	public Double generate() {
		boolean success = SamplingUtils.flip(outlierRate);
		if (success) {
			return generateOutlier();
		} else {
			return n.nextGaussian() * sigma + mean;
		}
	}

	@Override
	public String toString() {
		return "[" + mean + "," + sigma + "]";
	}


	/**
	 * an outlier is generated using the 1.5xIQR rule uniformly at random
	 * inside the interval: [q1-2*IQR, q1-1.5IQR)U[q3+1.5*IQR, q3+2*IQR)
	 *
	 * @return
	 */
	public double generateOutlier() {
		NormalDistribution dist = new NormalDistribution(mean, sigma);
		double q1 = dist.inverseCumulativeProbability(0.25);
		double q3 = dist.inverseCumulativeProbability(0.75);
		double iqr = q3 - q1;
		double outlier;
		if (SamplingUtils.flip(0.5)) {
			outlier = q3 + 1.5 * iqr + Math.random() * sigma;
		} else {
			outlier = q1 - 1.5 * iqr - Math.random() * sigma;
		}
		return outlier;
	}
}
