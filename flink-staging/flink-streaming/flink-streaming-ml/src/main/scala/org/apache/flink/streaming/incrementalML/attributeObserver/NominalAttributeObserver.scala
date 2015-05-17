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

class NominalAttributeObserver(
  val numberOfValues: Int)
  extends AttributeObserver[Metrics]
  with Serializable {

  // [AttributeVale,(#Yes,#No)]
  val attributeValues = mutable.HashMap[Double, (Double, Double)]()
  var instancesSeen: Double = 0.0 // instancesSeen

  override def getSplitEvaluationMetric: (Double, List[Double]) = {
    var entropy = 0.0
    for (attrValue <- attributeValues) {
      //E(attribute) = Sum { P(attrValue)*E(attrValue) }
      val valueCounter: Double = attrValue._2._1 + attrValue._2._2
      val valueProb: Double = valueCounter / instancesSeen
      var valueEntropy = 0.0

      if (attrValue._2._1 != 0.0) {
        valueEntropy += (attrValue._2._1 / valueCounter) * Utils.logBase2((attrValue._2._1 /
          valueCounter))
      }
      if (attrValue._2._2 != 0.0) {
        valueEntropy += (attrValue._2._2 / valueCounter) * Utils.logBase2((attrValue._2._2 /
          valueCounter))
      }
      entropy += (-valueEntropy) * valueProb
    }
    (entropy, attributeValues.keySet.toList)
  }

  /**
   *
   * @param inputAttribute
   */
  override def updateMetricsWithAttribute(inputAttribute: Metrics): Unit = {

    val VFDTAttribute = inputAttribute.asInstanceOf[VFDTAttributes]
    //if it's not the first time that we see the same value for e.g. attribute 1
    //attributes hashMap -> <attributeValue,(#1.0,#0.0)>
    instancesSeen += 1.0
    if (attributeValues.contains(VFDTAttribute.value)) {
      var temp = attributeValues.apply(VFDTAttribute.value)
      if (VFDTAttribute.label == -1.0) {
        temp = (temp._1, temp._2 + 1.0)
      }
      else {
        temp = (temp._1 + 1.0, temp._2)
      }
      attributeValues.put(VFDTAttribute.value, temp)
    }
    else {
      if (VFDTAttribute.label == -1.0) {
        attributeValues.put(VFDTAttribute.value, (0.0, 1.0))
      }
      else {
        attributeValues.put(VFDTAttribute.value, (1.0, 0.0))
      }
    }
  }

  override def toString: String = {
    s"$attributeValues"
  }
}
