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

case class NominalAttributeObserver(
  numberOfValues: Int,
  numberOfClasses: Int)
  extends AttributeObserver[Metrics]
  with Serializable {

  // [AttributeVale,(#class1,#class2,#class3,...)]
  val attributeValues = mutable.HashMap[Double, List[Int]]()
  var instancesSeen = 0.0D // instancesSeen

  override def getSplitEvaluationMetric: (Double, List[Double]) = {
    var entropy = 0.0D
    for (attrValue <- attributeValues) {
      //E(attribute) = Sum { P(attrValue)*E(attrValue) }

      val valueCounter = attrValue._2.sum.toDouble
      val valueProb = valueCounter / instancesSeen

      var valueEntropy = 0.0D

      attrValue._2.foreach(metric => {
        if (metric != 0) {
          valueEntropy -= (metric / valueCounter) * Utils.logBase2(metric / valueCounter)
        }
      })

      entropy += valueEntropy * valueProb
    }
    (entropy, attributeValues.keySet.toList)
  }

  override def updateMetricsWithAttribute(inputAttribute: Metrics): Unit = {

    val VFDTAttribute = inputAttribute.asInstanceOf[VFDTAttributes]
    //if it's not the first time that we see the same value for e.g. attribute 1
    //attributes hashMap -> <attributeValue,(#1.0,#0.0)>
    instancesSeen += 1.0
    val tempLabel = VFDTAttribute.label.toInt
    var tempList: List[Int] = null

    if (attributeValues.contains(VFDTAttribute.value)) {
      tempList = attributeValues.apply(VFDTAttribute.value)
    }
    else {
      tempList = List.fill(numberOfClasses)(0)
    }
    tempList = tempList.updated(tempLabel, tempList(tempLabel) + 1)
    attributeValues.put(VFDTAttribute.value, tempList)

  }

  override def toString: String = {
    s"$attributeValues"
  }
}
