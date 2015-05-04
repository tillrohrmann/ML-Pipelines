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
package org.apache.flink.streaming.incrementalML.classifier.classObserver

import org.apache.flink.streaming.incrementalML.classifier.{Metrics, VFDTAttributes}

import scala.collection.mutable

class NumericalAttributeObserver
  extends AttributeObserver[Metrics]
  with Serializable {

  var attributeSum = 0.0
  var attributeSoS = 0.0
  var attributeDistribution = (0.0, 0.0)
  //(#Yes,#No)
  var instancesSeen = 0

  var minValuePerClass = mutable.MutableList[Double](Double.MaxValue, Double.MaxValue)
  var maxValuePerClass = mutable.MutableList[Double](Double.MinValue, Double.MinValue)

  var attrMean = 0.0
  var attrStd = 0.0

  override def getSplitEvaluationMetric: Double = {
    return instancesSeen
  }

  override def updateMetricsWithAttribute(attr: Metrics): Unit = {
    val attribute = attr.asInstanceOf[VFDTAttributes]
    instancesSeen += 1
    attributeSum += attribute.value.asInstanceOf[Double]
    if (instancesSeen != 1) {
      val temp = instancesSeen * attribute.value.asInstanceOf[Double] - attributeSum
      attributeSoS += (1.0 / (instancesSeen * (instancesSeen - 1))) * (Math.pow(temp, 2))
    }

    attrMean = attributeSum / instancesSeen //update attribute mean
    attrStd = Math.sqrt(attributeSoS / instancesSeen) //update attribute std
    attributeDistribution =
      if (attribute.clazz == 0.0) (attributeDistribution._1, attributeDistribution._2 + 1.0)
      else (attributeDistribution._1 + 1.0, attributeDistribution._2)

    //--------------------------------------------------------------------------------------
    if (attribute.value.asInstanceOf[Double] < minValuePerClass(attribute.clazz
      .asInstanceOf[Int])) {
      minValuePerClass.update(attribute.clazz.asInstanceOf[Int], attribute.value
        .asInstanceOf[Double])
    }
    if (attribute.value.asInstanceOf[Double] > maxValuePerClass(attribute.clazz
      .asInstanceOf[Int])) {
      maxValuePerClass.update(attribute.clazz.asInstanceOf[Int], attribute.value
        .asInstanceOf[Double])
    }
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
    s"AttributeMin:$minValuePerClass, AttributeMax:$maxValuePerClass, " +
      s"AttributeDistribution:$attributeDistribution" +
      s"and all these just with $instancesSeen instances"
  }
}
