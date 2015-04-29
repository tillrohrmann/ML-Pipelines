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



class EvaluationMetric(
  val bestValue: (Int,Double),
  val secondBestValue: (Int,Double))
  extends Metrics
  with Serializable {

  override def toString: String = {
    s"Best attribute to Split: $bestValue, second best attribute to split:$secondBestValue\n"
  }
}

object EvaluationMetric {

  def apply(firstSplitAttr: (Int, Double), secondSplitAttr: (Int, Double)): EvaluationMetric = {
    new EvaluationMetric(firstSplitAttr,secondSplitAttr)
  }
}

class EvaluationMetricFunction(
  evalMetric: EvaluationMetricFunction)
  extends Serializable{

//  import EvaluationMetricFunction._

//  def calculateMetrics(): Double = evalMetric match {
//    case InformationGain => calculateInformationGain
//    case GiniImpurity => calculateGiniImpurity
//  }

  def calculateInformationGain: Double = {

    0.0
  }
//  def calculateGiniImpurity: Double ={
//
//    0.0
//  }
}

//object EvaluationMetricFunction extends Enumeration {
//  type EvaluationMetricFunction = Value
//  val GiniImpurity, InformationGain = Value
//}
