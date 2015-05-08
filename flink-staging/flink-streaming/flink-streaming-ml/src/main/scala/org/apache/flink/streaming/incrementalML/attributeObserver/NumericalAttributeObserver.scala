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
package org.apache.flink.streaming.incrementalML.attributeObserver

import org.apache.flink.streaming.incrementalML.classification.Metrics.{Metrics, VFDTAttributes}
import org.apache.flink.streaming.incrementalML.common.Utils

import scala.collection.mutable

class NumericalAttributeObserver
  extends AttributeObserver[Metrics]
  with Serializable {

  val SPLIT_POINTS_TO_CONSIDER = 3
  var attributeSum = 0.0
  var attributeSoS = 0.0
  var attributeDistribution = (0.0, 0.0) //(#Yes,#No)

  var instancesSeen = 0
  var instances = mutable.MutableList[(Double, Double)]() //(element,class)

  //(min,max) per class
  var minMaxValuePerClass = mutable.MutableList[(Double, Double)](
    (Double.MaxValue, Double.MinValue), (Double.MaxValue, Double.MinValue)
  )
  //  var maxValuePerClass = mutable.MutableList[Double](Double.MinValue, Double.MinValue)

  var attrMean = 0.0
  var attrStd = 0.0

  var minValueObserved = Double.MaxValue
  var maxValueObserved = Double.MinValue

  override def getSplitEvaluationMetric(): (Double, List[Double]) = {

    val potentialSplitPoints = Utils.getSplitPointsWithUniformApproximation(
      SPLIT_POINTS_TO_CONSIDER, minValueObserved, maxValueObserved)

    val metricsForSplitPoints = mutable.MutableList[Double]()
    var bestValueToSplit = (Double.MaxValue, List[Double]())

    //------------------ Decide Best Split Option ------------------------------------
    var entropy = 0.0
    for (point <- potentialSplitPoints) {
      var leftSide = (0.0, 0.0) //(#Yes,#No)
      var rightSide = (0.0, 0.0)

      for (instance <- instances) {
        if (instance._1 < point) {
          if (instance._2 == 0.0) {
            //check class
            leftSide = (leftSide._1, leftSide._2 + 1.0)
          }
          else {
            leftSide = (leftSide._1 + 1.0, leftSide._2)
          }
        }
        else {
          if (instance._2 == 0.0) {
            //check class
            rightSide = (rightSide._1, rightSide._2 + 1.0)
          }
          else {
            rightSide = (rightSide._1 + 1.0, rightSide._2)
          }
        }
      }
      //--------------------------------------------------
      for (metrics <- List(leftSide, rightSide)) {
        //E(attribute) = Sum { P(attrValue)*E(attrValue) }
        val valueCounter: Double = metrics._1 + metrics._2
        val valueProb: Double = valueCounter / instancesSeen
        var valueEntropy = 0.0

        if (metrics._1 != 0.0) {
          valueEntropy += (metrics._1 / valueCounter) * Utils.logBase2((metrics._1 /
            valueCounter))
        }
        if (metrics._2 != 0.0) {
          valueEntropy += (metrics._2 / valueCounter) * Utils.logBase2((metrics._2 /
            valueCounter))
        }
        entropy += (-valueEntropy) * valueProb
      }
      //--------------------------------------------------
      metricsForSplitPoints += entropy
      if (entropy < bestValueToSplit._1) {
        bestValueToSplit = (entropy, bestValueToSplit._2.::(point))
      }
    }
    instances.clear()
    bestValueToSplit
  }

  override def updateMetricsWithAttribute(attr: Metrics): Unit = {
    val attribute = attr.asInstanceOf[VFDTAttributes]
    val attributeValue = attribute.value.asInstanceOf[Double]
    calculateMeanAndStd(attributeValue)

    attributeDistribution =
      if (attribute.clazz == 0.0) (attributeDistribution._1, attributeDistribution._2 + 1.0)
      else (attributeDistribution._1 + 1.0, attributeDistribution._2)

    //--------------------------------------------------------------------------------------
    instances += ((attributeValue, attribute.clazz))

    //------------------------keep min and max per class-----------------------------
    if (attributeValue < minMaxValuePerClass(attribute.clazz.asInstanceOf[Int])._1) {
      minMaxValuePerClass.update(attribute.clazz.asInstanceOf[Int],
        (attributeValue, minMaxValuePerClass(attribute.clazz.asInstanceOf[Int])._2))
    }
    if (attributeValue > minMaxValuePerClass(attribute.clazz.asInstanceOf[Int])._2) {
      minMaxValuePerClass.update(attribute.clazz.asInstanceOf[Int],
        (minMaxValuePerClass(attribute.clazz.asInstanceOf[Int])._1, attributeValue))
    }
    //------------------------keep min and max per attribute-----------------------------
    if (attributeValue < minValueObserved) {
      minValueObserved = attributeValue
    }
    if (attributeValue > maxValueObserved) {
      maxValueObserved = attributeValue
    }
  }

  /** Calculates incrementally the gaussian mean and standard deviation of this numerical attribute.
    *
    * @param value the input value of the attribute
    */
  private def calculateMeanAndStd(value: Double): Unit = {
    instancesSeen += 1
    attributeSum += value
    if (instancesSeen != 1) {
      val temp = instancesSeen * value - attributeSum
      attributeSoS += (1.0 / (instancesSeen * (instancesSeen - 1))) * (Math.pow(temp, 2))
    }
    attrMean = attributeSum / instancesSeen //update attribute mean
    attrStd = Math.sqrt(attributeSoS / instancesSeen) //update attribute std
  }

  def getAttrMean: Double = {
    attrMean
  }

  def getAttrStd: Double = {
    attrStd
  }

  def getAttributeDistribution: (Double, Double) = {
    attributeDistribution
  }

  override def toString: String = {
    s"AttributeMinMax: $minMaxValuePerClass, " +
      s"AttributeDistribution: $attributeDistribution" +
      s"and all these just with $instancesSeen instances"
  }
}
