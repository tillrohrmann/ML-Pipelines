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

import org.apache.flink.ml.math.Vector
import org.apache.flink.streaming.incrementalML.classification.Metrics.AttributeType
import org.apache.flink.streaming.incrementalML.classification.Metrics.AttributeType.AttributeType

import scala.collection.mutable


object DecisionTreeModel
  extends Serializable {

  var decisionTree: mutable.HashMap[Int, DTNode] = null

  def createRootOfTheTree: Unit = {
    decisionTree.+=((0, DTNode(true, true, 0)))
  }

  /** Sorts the given data point to one of the tree's leaves.
    *
    * @param dataPointFeatures The features of the data point to be sorted with the Decision
    *                          tree as-is till now
    * @return The leaf, that this was sorted to
    */
  def classifyDataPointToLeaf(dataPointFeatures: Vector): Int = {
    //    var leaf = nodeId
    //    //classify data point and return leaf id
    //    var currentNode = this
    //    var tempChildrenList = children
    //    if (!currentNode.isLeaf) {
    //      if (currentNode.splitAttributeType == AttributeType.Numerical) {
    //        if (dataPointFeatures(splitAttribute).asInstanceOf[Double] < currentNode
    //          .attributeSplitValue) {
    //          currentNode = tempChildrenList.getOrElse("LeftHandSide", throw new RuntimeException
    //          ("Left Hand Side branch doesn't exist"))
    //          tempChildrenList = currentNode.children
    //          leaf = currentNode.nodeId
    //        }
    //        else{
    //          currentNode = tempChildrenList.getOrElse("RightHandSide", throw new RuntimeException
    //          ("Right Hand Side branch doesn't exist"))
    //          tempChildrenList = currentNode.children
    //          leaf = currentNode.nodeId
    //        }
    //      }
    //    }
    0
  }

  /** Grows a tree, meaning that it will split a leaf with the given attribute
    * that gives the maximum information gain
    *
    * @param leafToSplit The id of the leaf to be split
    * @param splitAttribute The id of the split attribute of the node
    * @param attrType The type of the attribute: either  [[AttributeType.Nominal]] or
    *                 [[AttributeType.Numerical]]
    * @param splitValue The Value of the splitting. Applies for numerical attributes
    * @param infoGain The information gain of this splitting
    */
  def growTree(leafToSplit: Int, splitAttribute: Int, attrType: AttributeType, splitValue: Double,
    infoGain: Double): Unit = {
    val nodeToSplit = decisionTree.getOrElse(leafToSplit, throw new RuntimeException("There is no" +
      " leaf to split with that Id"))
    nodeToSplit.splitNode(splitAttribute, attrType, splitValue, infoGain)
  }

  override def toString(): String = {
    s"DecisionTree:$decisionTree"
  }

}

/**
 *
 * @param isLeaf: True if a node is a Leaf
 * @param nodeId: A unique integer, identifying each one of the tree's nodes
 *
 */
case class DTNode(
  isRoot: Boolean,
  var isLeaf: Boolean,
  nodeId: Int)
  extends Serializable {

  /**
   * [[children]]: List of DecisionTreeModel branches of a Node of a Decision Tree. In case of
   * a continuous attribute, only two elements exist in the HashMap:
   * children(0) -> is the left hand side of the tree, for
   * attributeValue <= [[attributeSplitValue]].
   *
   * children(1) -> is the right hand side of the tree, for
   * attributeValue > [[attributeSplitValue]].
   *
   * [[splitAttribute]]: The Id of the splitting attribute
   * [[splitAttributeType]]: Two possible values: [[AttributeType.Nominal]] and
   * [[AttributeType.Numerical]]
   * [[attributeSplitValue]]: In case the splitting attribute is a Nominal attribute, this is
   * equal to NaN
   * [[informationGain]]: The information Gain that obtain by the selected splitting attribute
   * [[label]]: The Class that receives either majority vote or maximum probability in the
   * specific leaf
   *
   */
  var children: Option[mutable.HashMap[String, Int]] = None
  var splitAttribute: Option[Int] = None
  var splitAttributeType: Option[AttributeType] = None
  var attributeSplitValue = Double.NaN
  var informationGain = Double.NaN
  var label = Double.NaN

  /** The given node is split in two or more branches, by the use of  a Numerical
    * or Nominal attribute
    *
    * @param splitAttr The id of the split attribute of the node
    * @param splitAttrType The type of the attribute: either  [[AttributeType.Nominal]] or
    *                      [[AttributeType.Numerical]]
    * @param attrSplitValue The Value of the splitting. Applies for numerical attributes
    * @param infoGain The information gain of this splitting
    */
  def splitNode(splitAttr: Int, splitAttrType: AttributeType,
    attrSplitValue: Double, infoGain: Double): Unit = {
    //TODO:: Change attrSplitValue type to List[Double]
    children = Some(new mutable.HashMap[String, Int]())
    splitAttribute = Some(splitAttr)
    splitAttributeType = Some(splitAttrType)
    attributeSplitValue = attrSplitValue
    informationGain = infoGain
    if (isLeaf) {
      isLeaf = false
    }
  }

  override def toString(): String = {
    val s = new StringBuilder()
    s.append(s"NodeId:$nodeId -> children:$children")
    s.toString()
  }
}

