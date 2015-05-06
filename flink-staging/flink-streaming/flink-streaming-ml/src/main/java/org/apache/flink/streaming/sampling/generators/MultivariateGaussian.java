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

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.linear.NonPositiveDefiniteMatrixException;
import org.apache.commons.math3.linear.SingularMatrixException;
import org.apache.commons.math3.random.RandomGenerator;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by marthavk on 2015-03-27.
 */
public class MultivariateGaussian extends MultivariateNormalDistribution{


	public MultivariateGaussian(double[] means, double[][] covariances) throws SingularMatrixException, DimensionMismatchException, NonPositiveDefiniteMatrixException {
		super(means, covariances);
	}

	public MultivariateGaussian(Integer d) {
		//TODO
		super(new double[]{0},new double[][]{{0}});
		// randomly initialize means between [0,1)
		double[] means = new double[d];
		for (int i=1; i<d; i++) {
			means[i] = new Random().nextDouble();
		}
		// initialize diagonal covariances



	}

	public MultivariateGaussian(RandomGenerator rng, double[] means, double[][] covariances) throws SingularMatrixException, DimensionMismatchException, NonPositiveDefiniteMatrixException {
		super(rng, means, covariances);
	}

	public void updateMeans() {

	}
}
