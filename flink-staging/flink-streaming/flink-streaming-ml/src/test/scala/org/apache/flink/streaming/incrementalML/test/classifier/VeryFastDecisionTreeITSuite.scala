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
import org.apache.flink.streaming.incrementalML.classification.VeryFastDecisionTree
import org.apache.flink.streaming.incrementalML.common.StreamingMLUtils
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

import scala.util.hashing.MurmurHash3

class VeryFastDecisionTreeITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's Very Fast Decision Tree algorithm"

//  import VeryFastDecisionTreeData._

  it should "Create the classification HT of the given data set" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val vfdt = VeryFastDecisionTree(env)

    val parameters = ParameterMap()
//    val nominalAttributes = Map(0 -> 3, 1 -> 3, 2 -> 2)

    parameters.add(VeryFastDecisionTree.MinNumberOfInstances, 200)
    parameters.add(VeryFastDecisionTree.NumberOfClasses, 2)
//    parameters.add(VeryFastDecisionTree.NominalAttributes, nominalAttributes)

//    val dataPoints = data.map(point => {
//      val featuresVector = DenseVector.zeros(point._2.size)
//      for (i <- 0 until point._2.size) {
//        val value = point._2.apply(i)
//        value match {
//          case a: Double =>
//            featuresVector.update(i, a)
//          case a: String =>
//            featuresVector(i) = MurmurHash3.stringHash(a)
//          case _ =>
//        }
//      }
//      LabeledVector(point._1, featuresVector)
//    })

    val dataPoints = StreamingMLUtils.readLibSVM(env,"/Users/fobeligi/Downloads/decisionTreeTestData.t",123)

    vfdt.fit(dataPoints,parameters)
    env.execute()
  }
}

object VeryFastDecisionTreeData {

  val data: Seq[(Double, List[Any])] = List(
    (-1.0, List("Rainy", "Hot", "High", 4.00, -7.0, 0.0)),
    (-1.0, List("Rainy", "Hot", "High", 4.00, -12.0, 0.0)),
    (1.0, List("Overcast", "Hot", "High", -4.00, -3.0, 0.0)),
    (1.0, List("Sunny", "Mild", "High", -3.00, -2.0, 0.0)),
    (1.0, List("Sunny", "Cool", "Normal", -4.00, -1.0, 0.0)),
    (-1.0, List("Sunny", "Cool", "Normal", 4.00, -7.0, 0.0)),
    (1.0, List("Overcast", "Cool", "Normal", -2.00, -3.0, 0.0)),
    (-1.0, List("Rainy", "Mild", "High", 4.00, -11.0, 0.0)),
    (1.0, List("Rainy", "Cool", "Normal", -4.00, -3.0, 0.0)),
    (1.0, List("Sunny", "Mild", "Normal", -3.00, -6.0, 0.0)),
    (1.0, List("Rainy", "Mild", "Normal", -4.00, -4.0, 0.0)),
    (1.0, List("Overcast", "Mild", "High", -2.00, -3.0, 0.0)),
    (1.0, List("Overcast", "Hot", "Normal", -4.00, -2.0, 0.0)),
    (-1.0, List("Sunny", "Mild", "High", 4.00, -13.0, 0.0))
  )
}
