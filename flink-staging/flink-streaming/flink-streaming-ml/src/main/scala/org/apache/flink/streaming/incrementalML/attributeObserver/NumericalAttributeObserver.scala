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

  //  var attributeSum = 0.0
  //  var attributeSoS = 0.0
  //  var attributeDistribution = (0.0, 0.0) //(#Yes,#No)

  //  var instancesSeen = 0
  //  var instances = mutable.MutableList[(Double, Double)]() //(element,class)

  val SPLIT_POINTS_TO_CONSIDER = 10
  //(min,max) per class
  var minMaxValuePerClass = mutable.MutableList[(Double, Double)](
    (Double.MaxValue, Double.MinValue), (Double.MaxValue, Double.MinValue))

  //(instancesSeen, mean, std) per class
  var meanStdPerClass = mutable.MutableList[(Double, Double, Double)]((0, 0, 0), (0, 0, 0))

  //  var attrMean = 0.0
  //  var attrStd = 0.0

  var minValueObserved = Double.MaxValue
  var maxValueObserved = Double.MinValue

  override def getSplitEvaluationMetric(): (Double, List[Double]) = {

    val potentialSplitPoints = Utils.getSplitPointsWithUniformApproximation(
      SPLIT_POINTS_TO_CONSIDER, minValueObserved, maxValueObserved)

    //    val metricsForSplitPoints = mutable.MutableList[Double]()

    var bestValueToSplit = (Double.MaxValue, List[Double]())

    //------------------ Decide Best Split Option ------------------------------------
    //    var entropy = Double.MaxValue
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

        //              entropy -= (meanStdPerClass(i)._1 / totalInstancesSeen) * Utils.logBase2(
        //                meanStdPerClass(i)._1 / totalInstancesSeen)
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
      val entropy = calcEntropyOfChildren(leftHandSide, leftHandSideInstances, rightHandSide,
        rightHandSideInstances, totalInstancesSeen)

      if (entropy < bestValueToSplit._1) {
        bestValueToSplit = (entropy, List[Double](splitPoint))
      }

      //      var leftSide = (0.0, 0.0) //(#Yes,#No)
      //      var rightSide = (0.0, 0.0)

      //      for (instance <- instances) {
      //        if (instance._1 < point) {
      //          if (instance._2 == 0.0) {
      //            //check class
      //            leftSide = (leftSide._1, leftSide._2 + 1.0)
      //          }
      //          else {
      //            leftSide = (leftSide._1 + 1.0, leftSide._2)
      //          }
      //        }
      //        else {
      //          if (instance._2 == 0.0) {
      //            //check class
      //            rightSide = (rightSide._1, rightSide._2 + 1.0)
      //          }
      //          else {
      //            rightSide = (rightSide._1 + 1.0, rightSide._2)
      //          }
      //        }
      //      }
      //--------------------------------------------------
      //      for (metrics <- List(leftSide, rightSide)) {
      //        //E(attribute) = Sum { P(attrValue)*E(attrValue) }
      //        val valueCounter: Double = metrics._1 + metrics._2
      //        val valueProb: Double = valueCounter / instancesSeen
      //        var valueEntropy = 0.0
      //
      //        if (metrics._1 != 0.0) {
      //          valueEntropy += (metrics._1 / valueCounter) * Utils.logBase2((metrics._1 /
      //            valueCounter))
      //        }
      //        if (metrics._2 != 0.0) {
      //          valueEntropy += (metrics._2 / valueCounter) * Utils.logBase2((metrics._2 /
      //            valueCounter))
      //        }
      //        entropy += (-valueEntropy) * valueProb
      //      }
      //--------------------------------------------------
      //      metricsForSplitPoints += entropy
      //      if (entropy < bestValueToSplit._1) {
      //        bestValueToSplit = (entropy, bestValueToSplit._2.::(point))
      //      }
    }
    System.err.println(bestValueToSplit)
    bestValueToSplit
  }

  def calcEntropyOfChildren(lhs: mutable.HashMap[Int, Double], leftHSInstances: Double,
    rhs: mutable.HashMap[Int, Double], rightHSInstances: Double, totalInstances: Double): Double = {

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


  override def updateMetricsWithAttribute(attr: Metrics): Unit = {

    val attribute = attr.asInstanceOf[VFDTAttributes]

    updateMeanAndStd(attribute.value, attribute.clazz)

    //    attributeDistribution =
    //      if (attribute.clazz == 0.0) (attributeDistribution._1, attributeDistribution._2 + 1.0)
    //      else (attributeDistribution._1 + 1.0, attributeDistribution._2)
    //
    //    //--------------------------------------------------------------------------------------
    //    instances += ((attributeValue, attribute.clazz))

    //------------------------keep min and max per class-----------------------------
    val tempClass = attribute.clazz.asInstanceOf[Int]

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

  /** Calculates incrementally the sum of values and sum of squares (of the std formula), per class
    * for a numerical attribute.
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

    //    instancesSeen += 1
    //    attributeSum += value
    //    if (instancesSeen != 1) {
    //      val temp = instancesSeen * value - attributeSum
    //      attributeSoS += (1.0 / (instancesSeen * (instancesSeen - 1))) * (Math.pow(temp, 2))
    //    }
    //    attrMean = attributeSum / instancesSeen //update attribute mean
    //    attrStd = Math.sqrt(attributeSoS / instancesSeen) //update attribute std
  }

  def getMeanStdPerClass(clazz: Double): (Double, Double) = {
    val t = meanStdPerClass(clazz.asInstanceOf[Int])
    if (t._1 != 0.0) {
      return (t._2 / t._1, Math.sqrt(t._3 / t._1))
    }
    (t._2, t._3)
  }

  //  def getAttrStd: Double = {
  //    attrStd
  //  }

  //  def getAttributeDistribution: (Double, Double) = {
  //    attributeDistribution
  //  }

  override def toString: String = {
    s"AttributeMinMax: $minMaxValuePerClass, "
    //      s"AttributeDistribution: $attributeDistribution" +
    //      s"and all these just with $instancesSeen instances"
  }
}

object NumericalAttributeObserver {

  def main(args: Array[String]) {
    val temp = new NumericalAttributeObserver
    for (i <- 0 until data.size) {
      temp.updateMeanAndStd(data(i), 0)
    }
    println(s"Mean and srd for class 1: ${temp.getMeanStdPerClass(1)}")
  }

  val data: List[Double] = List(
    10.131415422021048
    , 9.490359302409315
    , 10.507908698618415
    , 9.256322941281223
    , 11.271070745775239
    , 9.980054949334944
    , 9.406411477222065
    , 9.270314245479097
    , 9.210533218451813
    , 9.35553400741161
    , 9.922817795798181
    , 9.999134000513864
    , 9.859220958535113
    , 13.15099325527155
    , 9.068076302982538
    , 10.380715024092277
    , 9.13421701373613
    , 9.027374327134174
    , 10.77374347837804
    , 11.310500784878341
    , 9.702772738867964
    , 9.856677085044591
    , 9.49544704939036
    , 9.950800404193933
    , 12.403094449057862
    , 8.854390929778628
    , 9.3097442845822
    , 10.668172728521348
    , 10.253521349566139
    , 10.80935770724536
    , 9.794352184526783
    , 8.72719725525249
    , 10.05001147032432
    , 11.445326079876047
    , 9.758737955659464
    , 9.28303361293171
    , 9.03119013736996
    , 10.167029650888367
    , 12.816473891267808
    , 10.205187753246207
    , 9.571763254106044
    , 10.301854945886072
    , 10.720322135077064
    , 8.981584604304764
    , 8.538950616953807
    , 9.810887362215182
    , 8.98540041454055)
}
