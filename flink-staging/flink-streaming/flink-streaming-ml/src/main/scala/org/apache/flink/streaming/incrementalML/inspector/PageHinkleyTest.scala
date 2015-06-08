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
package org.apache.flink.streaming.incrementalML.inspector

import org.apache.flink.ml.common.{ParameterMap, Parameter}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.incrementalML.inspector.PageHinkleyTest.{Delta, Lambda,
MinInstances}

/**
 * Page Hinkley Test implementation of [[ChangeDetector]]. Detects changes
 *
 */
class PageHinkleyTest
  extends ChangeDetector[Double, Boolean]
  with Serializable {

  def setLambda(lambda: Double): PageHinkleyTest = {
    parameters.add(Lambda, lambda)
    this
  }

  def setDelta(delta: Double): PageHinkleyTest = {
    parameters.add(Delta, delta)
    this
  }

  def setMinInstances(minInstances: Double): PageHinkleyTest = {
    parameters.add(MinInstances, minInstances)
    this
  }

  /** Adding another observation to the change detector.
    * Change detector's output is updated with the new data point.
    *
    * @param inputPoints the new input point to change detector
    * @return True if a change was detected
    */
  override def input(inputPoints: DataStream[Double], inspectorParameters: ParameterMap):
  DataStream[Boolean] = {

    val resultingParameters = this.parameters ++ inspectorParameters

    val delta = resultingParameters.apply(delta)
    val lambda = resultingParameters.apply(lambda)

    var mean: Double = 0.0
    var pointsSeen: Int = 0
    var cumulativeSum: Double = 0
    var minValue: Double = Double.MaxValue
    var changeDetected = false

    inputPoints.map{point =>
      pointsSeen +=1
      mean += (point - mean) / pointsSeen
      cumulativeSum += point - mean - delta

      //TODO::Possible refactoring -> start checking for change detection after a # of instances
      // have been seen
      //if (pointsSeen > minInstances)
      // update minValue if needed
      if (cumulativeSum < minValue) {
        minValue = cumulativeSum
      }
      if (cumulativeSum - minValue > lambda) {
        changeDetected = true
        //change detected  -> reset change detector
        minValue = Double.MaxValue
        mean = 0.0
        pointsSeen = 0
        cumulativeSum = 0.0
      }
      changeDetected
    }
  }

//  /** Copies the ChangeDetector instance
//    *
//    * @return Copy of the ChangeDetector instance
//    */
//  override def copy(): ChangeDetector = {
//    val newChangeDetector = PageHinkleyTest(lambda, delta, minInstances)
//    newChangeDetector.mean = this.mean
//    newChangeDetector.pointsSeen = this.pointsSeen
//    newChangeDetector.cumulativeSum = this.cumulativeSum
//    newChangeDetector.minValue = this.minValue
//    newChangeDetector.isChangedDetected = this.isChangedDetected
//    newChangeDetector
//  }
//
}

object PageHinkleyTest {

  // ====================================== Parameters =============================================

  case object Lambda extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.0)
  }

  case object Delta extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.0)
  }

  case object MinInstances extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(30)
  }

  // ========================= Factory methods =====================================

  def apply(): PageHinkleyTest = {
    new PageHinkleyTest()
  }
}
