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
package org.apache.flink.streaming.scala.examples.incrementalML.classifier

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{LabeledVector, Parameter, WithParameters}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import VeryFastDecisionTree._
import org.apache.flink.util.Collector

class VeryFastDecisionTree(context: StreamExecutionEnvironment)
  extends Serializable
  with WithParameters {

  def setMinNumberOfInstances(minInstances: Int): VeryFastDecisionTree = {
    parameters.add(minNumberOfInstances, minInstances)
    this
  }

  def setThreshold(thresh: Double): VeryFastDecisionTree = {
    parameters.add(threshold, thresh)
    this
  }

  val dataPointsStream = context.fromCollection(data).map(dp => DataPoints(dp))
  dataPointsStream.
}

object VeryFastDecisionTree {

  /** Minimum number of instances seen, before deciding the new splitting feature.
    *
    */
  case object minNumberOfInstances extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(200)
  }

  /** Hoeffding Bound threshold
    *
    */
  case object threshold extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(1.0)
  }

  def apply(context: StreamExecutionEnvironment): VeryFastDecisionTree = {
    new VeryFastDecisionTree(context)
  }

  val data: Seq[LabeledVector] = List(LabeledVector(1.0, DenseVector(Array(2104.00, 3.00))),
    LabeledVector(1.0, DenseVector(Array(1600.00, 3.00))),
    LabeledVector(1.0, DenseVector(Array(2400.00, 3.00)))
  )

  def main(args: Array[String]) {
    val con = StreamExecutionEnvironment.getExecutionEnvironment
    VeryFastDecisionTree(con)
    con.execute()
  }
}
