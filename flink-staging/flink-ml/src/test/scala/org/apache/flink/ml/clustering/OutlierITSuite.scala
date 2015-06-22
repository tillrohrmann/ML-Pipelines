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
package org.apache.flink.ml.clustering

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class OutlierITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "The KMeans implementation"

  def fixture = new {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val kmeans = KMeans().setInitialCentroids(env.fromCollection(Clustering.centroidData)).
      setNumIterations(Clustering.iterations).setThreshold(0.20)

    val trainingDS = env.fromCollection(Clustering.trainingData)

    kmeans.fit(trainingDS)
  }

  it should "predict points to cluster centers" in {
    val f = fixture

    val vectorsWithExpectedLabels = Clustering.testData
    // create a lookup table for better matching
    val expectedMap = vectorsWithExpectedLabels map (v =>
      v.vector.asInstanceOf[DenseVector] -> v.label
      ) toMap

    // calculate the vector to cluster mapping on the plain vectors
    val plainVectors = vectorsWithExpectedLabels.map(v => v.vector)
    val predictedVectors = f.kmeans.predict(f.trainingDS)

    predictedVectors.print()
  }
}
