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

import breeze.stats.distributions.Gaussian
import org.apache.flink.streaming.incrementalML.classification.Metrics.{Metrics, VFDTAttributes}
import org.apache.flink.streaming.incrementalML.common.Utils

import scala.collection.mutable

class NumericalAttributeObserver
  extends AttributeObserver[Metrics]
  with Serializable {

  val SPLIT_POINTS_TO_CONSIDER = 10

  //(min,max) per class
  var minMaxValuePerClass = mutable.MutableList[(Double, Double)](
    (Double.MaxValue, Double.MinValue), (Double.MaxValue, Double.MinValue), (Double.MaxValue,
      Double.MinValue)
  )

  //(instancesSeen, mean, std) per class: 0, 1, 2
  var meanStdPerClass = mutable.MutableList[(Double, Double, Double)]((0, 0, 0), (0, 0, 0), (0,
    0, 0))

  //TODO:: replace below variables with a function that calculates those from the per class metrics
  var minValueObserved = Double.MaxValue
  var maxValueObserved = Double.MinValue

  override def getSplitEvaluationMetric(): (Double, List[Double]) = {

    val potentialSplitPoints = Utils.getSplitPointsWithUniformApproximation(
      SPLIT_POINTS_TO_CONSIDER, minValueObserved, maxValueObserved)

    var bestValueToSplit = (Double.MaxValue, List[Double]())

    //------------------ Decide Best Split Option ------------------------------------
    val gaussianDistributionPerClass = mutable.HashMap[Int, Gaussian]()
    var totalInstancesSeen = 0.0

    for (i <- 0 until meanStdPerClass.size) {
      totalInstancesSeen += meanStdPerClass(i)._1
      val meanStd = getMeanStdPerClass(i)
      if (meanStd._2 != 0.0) {
        gaussianDistributionPerClass.+=((i, new Gaussian(meanStd._1, meanStd._2)))
      }
    }

    for (splitPoint <- potentialSplitPoints) {

      val leftHandSide = new mutable.HashMap[Int, Double]()
      val rightHandSide = new mutable.HashMap[Int, Double]()

      var leftHandSideInstances = 0.0
      var rightHandSideInstances = 0.0

      for (i <- 0 until meanStdPerClass.size) {
        val distribution = gaussianDistributionPerClass.getOrElse(i, None)
        //        System.err.println(s"class:$i")
        distribution match {
          case distr: Gaussian => {
            //splitPoint is less than min value of this class
            if (splitPoint < minMaxValuePerClass(i)._1) {
              rightHandSide.+=((i, meanStdPerClass(i)._1))
              rightHandSideInstances += meanStdPerClass(i)._1
            } //splitPoint is greater than max value of this class
            else if (splitPoint >= minMaxValuePerClass(i)._2) {
              leftHandSide.+=((i, meanStdPerClass(i)._1))
              leftHandSideInstances += meanStdPerClass(i)._1
            } //splitPoint is between min-max value of this class
            else {
              val instancesToLHS = distr.cdf(splitPoint) * meanStdPerClass(i)._1
              leftHandSide.+=((i, instancesToLHS))
              leftHandSideInstances += instancesToLHS
              rightHandSide.+=((i, meanStdPerClass(i)._1 - instancesToLHS))
              rightHandSideInstances += meanStdPerClass(i)._1 - instancesToLHS
            }
          }
          case None =>
        }
      }
      val entropy = if (leftHandSideInstances + rightHandSideInstances != 0.0) {
        calcEntropyOfChildren(leftHandSide, leftHandSideInstances, rightHandSide,
          rightHandSideInstances, totalInstancesSeen)
      }
      else {
        Double.MaxValue
      }

      if (entropy < bestValueToSplit._1) {
        bestValueToSplit = (entropy, List[Double](splitPoint))
      }
    }
    bestValueToSplit
  }

  def calcEntropyOfChildren(lhs: mutable.HashMap[Int, Double], leftHSInstances: Double,
    rhs: mutable.HashMap[Int, Double], rightHSInstances: Double,
    totalInstances: Double):
  Double = {

    var leftHSEntropy = 0.0
    var rightHSEntropy = 0.0

    lhs.foreach(l => {
      leftHSEntropy -= (l._2 / leftHSInstances) * Utils.logBase2(l._2 / leftHSInstances)
    })

    rhs.foreach(r => {
      rightHSEntropy -= (r._2 / rightHSInstances) * Utils.logBase2(r._2 / rightHSInstances)
    })

    val entropy = (leftHSInstances / totalInstances) * leftHSEntropy + (rightHSInstances /
      totalInstances) * rightHSEntropy

    entropy
  }

  def getMeanStdPerClass(clazz: Double): (Double, Double) = {
    val t = meanStdPerClass(clazz.asInstanceOf[Int])
    if (t._1 != 0.0) {
      return (t._2 / t._1, Math.sqrt(t._3 / t._1))
    }
    (t._2, t._3)
  }

  override def updateMetricsWithAttribute(attr: Metrics): Unit = {

    val attribute = attr.asInstanceOf[VFDTAttributes]

    updateMeanAndStd(attribute.value, attribute.label)

    //------------------------keep min and max per class-----------------------------
    val tempClass = attribute.label.asInstanceOf[Int]

    if (attribute.value < minMaxValuePerClass(tempClass)._1) {
      minMaxValuePerClass.update(tempClass, (attribute.value, minMaxValuePerClass(tempClass)._2))
    }
    if (attribute.value > minMaxValuePerClass(tempClass)._2) {
      minMaxValuePerClass.update(tempClass, (minMaxValuePerClass(tempClass)._1, attribute.value))
    }
    //------------------------keep min and max per attribute-----------------------------
    if (attribute.value < minValueObserved) {
      minValueObserved = attribute.value
    }
    if (attribute.value > maxValueObserved) {
      maxValueObserved = attribute.value
    }
  }

  /** Calculates incrementally the sum of values and sum of squares (of the std formula),
    * per class for a numerical attribute.
    *
    * @param value the input value of the attribute
    * @param label the label of the attribute
    */
  private def updateMeanAndStd(value: Double, label: Double): Unit = {

    //(instancesSeen, attrSum, attrSumOfSquares) per class
    val metricsTemp = meanStdPerClass(label.asInstanceOf[Int])
    val instancesTemp = metricsTemp._1 + 1.0
    val attrSumTemp = metricsTemp._2 + value
    var attrSoSTemp = metricsTemp._3

    if (instancesTemp != 1.0) {
      val temp = instancesTemp * value - attrSumTemp
      attrSoSTemp += (1.0 / (instancesTemp * (instancesTemp - 1))) * (Math.pow(temp, 2))
    }
    meanStdPerClass.update(label.asInstanceOf[Int], (instancesTemp, attrSumTemp, attrSoSTemp))
  }

  override def toString: String = {
    s"AttributeMinMax per class: $minMaxValuePerClass, attribute mean and std " +
      s"per class: $meanStdPerClass"
  }
}
