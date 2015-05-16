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
package org.apache.flink.streaming.incrementalML.common

import breeze.stats.distributions.Gaussian

import scala.collection.mutable

object Utils {

  def logBase2(num: Double): Double = {
    math.log10(num) / math.log10(2)
  }

  def getSplitPointsWithUniformApproximation(splitPoints: Int, minValue: Double, maxValue: Double):
  mutable.MutableList[Double] = {
    val result = mutable.MutableList[Double]()

    //    println(s"-----#instances:$instancesSeen, mean:$attrMean, std: $attrStd")

    for (i <- 0 until splitPoints) {
      val t = (maxValue - minValue) / (splitPoints + 1)
        .asInstanceOf[Double]
      val temp = minValue + (i + 1) * t
      //      println(s"temp:$temp, minValue:$minValueObserved, maxValue:$maxValueObserved")
      if (temp >= minValue && temp <= maxValue && !result.contains(temp)) {
        result += temp
      }
    }
    result
  }

  def getSplitPointsWithGaussianApproximation(splitPoints: Int, minValue: Double, maxValue: Double,
    attrMean: Double, attrStd: Double): mutable.MutableList[Double] = {
    val result = mutable.MutableList[Double]()

    //    println(s"-----#instances:$instancesSeen, mean:$attrMean, std: $attrStd")

    val gaussianDistribution = new Gaussian(attrMean, attrStd)

    for (i <- 0 until splitPoints) {
      val t = (i + 1).asInstanceOf[Double] / (splitPoints + 1).asInstanceOf[Double]
      val temp = attrMean + attrStd * gaussianDistribution.icdf(t)
      //      println(s"temp:$temp, minValue:$minValueObserved, maxValue:$maxValueObserved")

      if (temp >= minValue && temp <= maxValue && !result.contains(temp)) {
        result += temp
      }
    }
    result
  }
}
