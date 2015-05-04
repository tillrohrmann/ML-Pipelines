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
package org.apache.flink.streaming.incrementalML.classification

import org.apache.flink.streaming.incrementalML.classification.Metrics.AttributeType.AttributeType
import org.apache.flink.streaming.incrementalML.classification.Metrics.{AttributeType, DataPoints}

import scala.collection.mutable

/**
 *
 * @param isLeaf: True if a node is a Leaf
 * @param nodeId: A unique integer, identifying each one of the tree's nodes
 * @param children: List of DecisionTreeModel branches of a Node of a Decision Tree. In case of
 *                a continuous attribute, only two elements exist in the list:
 *                children(0) -> is the left hand side of the tree, for
 *                attributeValue < [[attributeSplitValue]].
 *
 *                children(1) -> is the right hand side of the tree, for
 *                attributeValue >= @[[attributeSplitValue]].
 * @param splitAttribute: The Id of the splitting attribute
 * @param splitAttributeType: Two possible values: [[AttributeType.Nominal]] and
 *                          [[AttributeType.Numerical]]
 * @param attributeSplitValue: In case the splitting attribute is a Nominal attribute, this is
 *                           equal to NaN
 * @param informationGain: The information Gain that obtain by the selected splitting attribute
 * @param label: The Class that receives either majority vote or maximum probability in the
 *             specific leaf
 */
class DecisionTreeModel(
  val isLeaf: Boolean = true,
  val nodeId: Int,
  val children: mutable.MutableList[DecisionTreeModel] = mutable.MutableList[DecisionTreeModel](),
  val splitAttribute: Option[Int],
  val splitAttributeType: Option[AttributeType],
  val attributeSplitValue: Option[Double],
  val informationGain: Option[Double],
  val label: Option[Double])
  extends Serializable {

  /**
   *
   * @param dataPoint The dataPoint to be Classified with the Decision tree as-is till now
   * @return The leaf, that this was classified to
   */
  def classifyDataPoint(dataPoint: DataPoints): Int = {
    //classify data point and return leaf id
    1
  }

  override def toString(): String = {
    s"To Be Implemented"
  }
}

object DecisionTreeModel {
  def apply(leaf: Boolean, id: Int, children: mutable.MutableList[DecisionTreeModel], splitAttr:
  Int,
    splitAttrType: AttributeType, attrSplitValue: Double, infoGain: Double, label: Double):
  DecisionTreeModel = {
    new DecisionTreeModel(leaf, id, children, splitAttr, splitAttrType, attrSplitValue, infoGain,
      label)
  }
}
