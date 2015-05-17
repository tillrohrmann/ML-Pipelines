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
package org.apache.flink.streaming.incrementalML.test.classifier

import org.apache.flink.ml.common.{LabeledVector, ParameterMap}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.incrementalML.classification.Metrics.Metrics
import org.apache.flink.streaming.incrementalML.classification.VeryFastDecisionTree
import org.apache.flink.streaming.incrementalML.common.ChainedLearner
import org.apache.flink.streaming.incrementalML.preprocessing.Imputer
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

import scala.util.hashing.MurmurHash3

class VeryFastDecisionTreeITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's Very Fast Decision Tree algorithm"

  it should "Create the classification HT of the given data set" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val parameters = ParameterMap()
    val nominalAttributes = Map(1 -> 3, 3 -> 3, 5 -> 2, 6 -> 0, 7 -> 0, 8 -> 0, 9 -> 0, 13 -> 0,
      14 -> 0)

    parameters.add(VeryFastDecisionTree.MinNumberOfInstances, 200)
    parameters.add(VeryFastDecisionTree.NumberOfClasses, 2)
    parameters.add(VeryFastDecisionTree.NominalAttributes, nominalAttributes)

    val datapoints = env.readTextFile("/Users/fobeligi/Documents/dataSets/adultsTrainDataSet.csv").
      map {
      line => {
        var featureList = Vector[Double]()
        val features = line.split(',')
        for (i <- 0 until features.size - 1) {
          features(i).trim match {
            case "?" =>
              featureList = featureList :+ Double.NaN
            case _ => {
              if (nominalAttributes.contains(i)) {
                featureList = featureList :+ MurmurHash3.stringHash(features(i)).toDouble
              }
              else {
                featureList = featureList :+ features(i).toDouble
              }
            }
          }
        }
        val vector = if (features(features.size - 1).trim equals ">50K") {
          LabeledVector(1, DenseVector(featureList.toArray))
        }
        else {
          LabeledVector(-1, DenseVector(featureList.toArray))
        }
        vector
      }
    }

    //    val dataPoints = StreamingMLUtils.readLibSVM(env,
    //      "/Users/fobeligi/Downloads/decisionTreeTestData.t", 123)

    val transformer = Imputer()
    val vfdtLearner = VeryFastDecisionTree(env)
    val vfdtChainedLearner = new ChainedLearner[LabeledVector, LabeledVector, Metrics] (transformer,
      vfdtLearner)

    vfdtChainedLearner.fit(datapoints, parameters)
    env.execute()
  }
}
