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

  //  def createJSON_VFDT(intToNode: mutable.Map[Int, DTNode], rootNodeId: Int) = {
  //
  //    val rootNode = intToNode.getOrElse(rootNodeId, None)
  //
  //    rootNode match {
  //      case rn: DTNode => {
  //        var jsonString = new StringBuilder("{ ")
  //
  //        val stack = new mutable.Stack[Int]()
  //        val discovered = new mutable.Stack[Int]()
  //
  //        stack.push(rootNodeId)
  //
  //        while (!stack.isEmpty) {
  //          val node = stack.pop()
  //          if (!discovered.contains(node)) {
  //
  //            jsonString ++= ( s"id: \"node$node\", name: \"$node\", " +
  //              s"data: {splitting attribute: ${intToNode.get(node).get.splitAttribute.get}, " +
  //              s"splitting value:${intToNode.get(node).get.attributeSplitValue.get} }, " +
  //              s"children: [ ")
  //
  //            discovered.push(node)
  //
  //            rn.children.get match {
  //              case None =>
  //              case kids : mutable.Map[Double,Int] => {
  //                kids.foreach(
  //                  child => {
  //                    stack.push(child._2)
  //                    val tempNode = intToNode.get(child._2).get
  //                    jsonString ++= ( s"{ id: \"node${child._2}\", name: \"${child._2}\", " +
  //                      s"data: {splitting attribute: ${tempNode.splitAttribute.get}, " +
  //                      s"splitting value:${tempNode.attributeSplitValue.get} , ")
  //                  })
  //                if (kids.size != 0) {
  //                  jsonString ++= ("}")
  //                }
  //                jsonString ++= ("]")
  //              }
  //            }
  //          }
  //        }
  //      }
  //      case None =>
  //    }
  //  }

}
