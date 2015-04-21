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
package org.apache.flink.streaming.scala.examples.incrementalML.inspector

/**
 * Page Hinkley Test implementation of [[ChangeDetector]]. Detects changes
 *
 * @param delta delta parameter of PHT
 * @param lambda lambda parameter of PHT
 * @param minInstances minimum number of instances that are required before starting to detect
 *                     change
 */
class PageHinkleyTest(
    val lambda: Double,
    val delta: Double,
    val minInstances: Int)
  extends ChangeDetector
  with Serializable {

  var mean: Double = 0.0
  var pointsSeen: Int = 0
  var cumulativeSum: Double = 0
  var minValue: Double = Double.MaxValue

  /** Adding another observation to the change detector
    *
    * Change detector's output is updated with the new data point
    *
    * @param inputPoint the new input point to change detector
    *
    * @return True if a change was detected
    */
  override def input(inputPoint: Double): Boolean = {
    pointsSeen += 1
    mean += (inputPoint - mean) / pointsSeen
    cumulativeSum += inputPoint - mean - delta

    //TODO::Possible refactoring -> start checking for change detection after a # of instances have been seen
    //if (pointsSeen > minInstances)
      // update minValue if needed
    if (cumulativeSum < minValue) {
      minValue = cumulativeSum
    }
    if (cumulativeSum - minValue > lambda) {
      isChangedDetected = true
      return true
    }
    false
  }

  /** Copies the ChangeDetector instance
    *
    * @return Copy of the ChangeDetector instance
    */
  override def copy(): ChangeDetector = {
    val newChangeDetector = PageHinkleyTest(lambda, delta, minInstances)
    newChangeDetector.mean = this.mean
    newChangeDetector.pointsSeen = this.pointsSeen
    newChangeDetector.cumulativeSum = this.cumulativeSum
    newChangeDetector.minValue = this.minValue
    newChangeDetector.isChangedDetected = this.isChangedDetected
    newChangeDetector
  }

  /** Resets the change detector.
    *
    */
  override def reset(): Unit = {
    isChangedDetected = false
    minValue = Double.MaxValue
    mean = 0.0
    pointsSeen = 0
    cumulativeSum = 0.0
  }

  override def toString: String = {
    val result = StringBuilder.newBuilder
    result.append("Page Hinkley Test change Detector with:\n")
    result.append(s"lambda=$lambda\tdelta=$delta\tminNumberOfInstances=$minInstances\n")
    result.append("As till now:\n")
    result.append(s"pointsSeen=$pointsSeen\t detectedChange=$isChangedDetected")
    result.toString()
  }
}

object PageHinkleyTest {

  // ========================= Factory methods =====================================

  def apply(): PageHinkleyTest = {
    new PageHinkleyTest(0.0, 0.0, 30)
  }

  def apply(lambda: Double, delta: Double, numInstances: Int): PageHinkleyTest = {
    new PageHinkleyTest(lambda, delta, numInstances)
  }
}
