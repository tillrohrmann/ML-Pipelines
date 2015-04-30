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

class NominalAttributeObserver
  extends AttributeObserver[Metrics]
  with Serializable {

  val attributeValues = mutable.HashMap[String, (Double, Double)]()

  /**
   *
   * @return
   */
  override def getSplitEvaluationMetric: Double = {
    return 0.0
  }

  /**
   *
   * @param inputAttribute
   */
  override def updateMetricsWithAttribute(inputAttribute: Metrics): Unit = {

    val VFDTAttribute = inputAttribute.asInstanceOf[VFDTAttributes]
    //if it's not the first time that we see the same value for e.g. attribute 1
    //attributes hashMap -> <attributeValue,(#1.0,#0.0)>
    if (attributeValues.contains(VFDTAttribute.value.toString)) {
      var temp = attributeValues.apply(VFDTAttribute.value.toString)
      temp = if (VFDTAttribute.clazz == 0.0) (temp._1, temp._2 + 1.0) else (temp._1 + 1.0, temp._2)
      attributeValues.put(VFDTAttribute.value.toString, temp)
    }
    else {
      if (VFDTAttribute.clazz == 0.0) {
        attributeValues.put(VFDTAttribute.value.toString, (0.0, 1.0))
      }
      else {
        attributeValues.put(VFDTAttribute.value.toString, (1.0, 0.0))
      }

    }
  }

  override def toString: String = {
    s"$attributeValues"
  }
}

