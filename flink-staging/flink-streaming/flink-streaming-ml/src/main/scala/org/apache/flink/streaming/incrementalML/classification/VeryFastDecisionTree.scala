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

import java.lang.Iterable
import java.util

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction}
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.incrementalML.Learner
import org.apache.flink.streaming.incrementalML.attributeObserver.{AttributeObserver, NominalAttributeObserver, NumericalAttributeObserver}
import org.apache.flink.streaming.incrementalML.classification.Metrics._
import org.apache.flink.streaming.incrementalML.classification.VeryFastDecisionTree._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 *
 * @param context
 */
class VeryFastDecisionTree(
  context: StreamExecutionEnvironment)
  extends Learner[LabeledVector, Metrics]
  with Serializable {

  //TODO:: Check what other parameters need to be set
  def setMinNumberOfInstances(minInstances: Int): VeryFastDecisionTree = {
    parameters.add(MinNumberOfInstances, minInstances)
    this
  }

  def setThreshold(thresh: Double): VeryFastDecisionTree = {
    parameters.add(Threshold, thresh)
    this
  }

  def setNominalAttributes(noNominalAttrs: Map[Int,Int]): VeryFastDecisionTree ={
    parameters.add(NominalAttributes, noNominalAttrs)
    this
  }

  def setNumberOfClasses(noClasses: Int): VeryFastDecisionTree ={
    parameters.add(NumberOfClasses, noClasses)
    this
  }

  override def fit(input: DataStream[LabeledVector], fitParameters: ParameterMap):
  DataStream[Metrics] = {
    val resultingParameters = this.parameters ++ fitParameters

    val dataPointsStream: DataStream[Metrics] = input.map(dp => DataPoints(dp))
    val out = dataPointsStream.iterate[Metrics](10000)(dataPointsStream => iterationFunction
      (dataPointsStream))
    out
  }

  private def iterationFunction(dataPointsStream: DataStream[Metrics]): (DataStream[Metrics],
    DataStream[Metrics]) = {

    val mSAds = dataPointsStream.flatMap(new GlobalModelMapper).setParallelism(1)

    //TODO:: Decide which values will declare if it is a Model or a Signal
    val attributes = mSAds.filter(new FilterFunction[(Long, Metrics)] {
      override def filter(value: (Long, Metrics)): Boolean = {
        return value._1 >= 0
      }
    })

    val modelAndSignal = mSAds.filter(new FilterFunction[(Long, Metrics)] {
      override def filter(value: (Long, Metrics)): Boolean = {
        return (value._1 == -1 || value._1 == -2) //metric or Signal
      }
    })

    val splitDs = attributes.groupBy(0).merge(modelAndSignal.broadcast)
      .flatMap(new PartialVFDTMetricsMapper).setParallelism(1).split(new OutputSelector[Metrics] {
      override def select(value: Metrics): Iterable[String] = {
        val output = new util.ArrayList[String]()

        if (value.isInstanceOf[EvaluationMetric]) {
          output.add("feedback")
        }
        else {
          output.add("output")
        }
        output
      }
    })

    val feedback: DataStream[Metrics] = splitDs.select("feedback")
    val output: DataStream[Metrics] = splitDs.select("output")
    (feedback, output)
  }

}

object VeryFastDecisionTree {

  /** Minimum number of instances seen, before deciding the new splitting feature.
    *
    */
  case object MinNumberOfInstances extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(200)
  }

  /** Hoeffding Bound threshold
    *
    */
  case object Threshold extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(1.0)
  }

  /** Map that specifies which attributes are Nominal and how many possible values they will have
    *
    */
  case object NominalAttributes extends Parameter[Map[Int,Int]] {
    override val defaultValue: Option[Map[Int,Int]] = Some(Map[Int,Int]())
  }

  /**
   * Specifies the number of classes that the problem to be solved will have
   */
  case object NumberOfClasses extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(2)
  }

  def apply(context: StreamExecutionEnvironment): VeryFastDecisionTree = {
    new VeryFastDecisionTree(context)
  }
}

//object VeryFastDecisionTreeModel extends Serializable {
//
//}

/**
 *
 */
class GlobalModelMapper extends FlatMapFunction[Metrics, (Long, Metrics)] {
  var counter = 0.0
  val VFDT = DecisionTreeModel
  VFDT.createRootOfTheTree
  println(s"---------------------My decision tree:$VFDT")

  override def flatMap(value: Metrics, out: Collector[(Long, Metrics)]): Unit = {

    //if a data point is received
    if (value.isInstanceOf[DataPoints]) {
      val newDataPoint = value.asInstanceOf[DataPoints]
      counter += 1.0
      //TODO:: 1. classify data point first
      //TODO:: 2. number of instances seen till now
      val leafId = VFDT.classifyDataPointToLeaf(newDataPoint.getFeatures)
      val tempVector = newDataPoint.getFeatures

//      for (i <- 0 until tempVector.size) {
//        if (tempVector.apply(i).isInstanceOf[String]) {
//          out.collect((i, VFDTAttributes(i, tempVector(i).asInstanceOf[String],
//            newDataPoint.getLabel, 10, AttributeType.Nominal)))
//        }
//        else {
//          out.collect((i, VFDTAttributes(i, tempVector.apply(i), newDataPoint.getLabel, leafId,
//            AttributeType.Numerical)))
//        }
//      }
      if (counter == 14.0) {
        println("-----------------Signal----------------------------")
        out.collect((-2, CalculateMetricsSignal(10)))
      }
    } //metrics are received, then update global model
    else if (value.isInstanceOf[EvaluationMetric]) {
      //TODO:: Aggregate metrics and update global model. Do NOT broadcast global model
      println("------------------------------Metric-------------------------------------" + value)
    }
    else {
      throw new RuntimeException("--------------------WTF is that, that you're " +
        "sending me ??? x-( :" + value.getClass.toString)
    }
  }
}

/**
 *
 */
class PartialVFDTMetricsMapper extends FlatMapFunction[(Long, Metrics), Metrics] {
  //[LeafId,HashMap[AttributeId,AttributeObserver]]
  val leafsObserver = new mutable.HashMap[Int, mutable.HashMap
    [Long, AttributeObserver[Metrics]]]()

  //        var leafClassTemp: mutable.HashMap[String, mutable.HashMap[Long,
  //          AttributeObserver[Metrics]]] = null

  var attributesObserverTemp: mutable.HashMap[Long, AttributeObserver[Metrics]] = null

  var bestAttributesToSplit = mutable.MutableList[(Long, (Double, Double))]()

  override def flatMap(value: (Long, Metrics), out: Collector[Metrics]): Unit = {

    //TODO:: if attribute just do update the metrics
    if (value._2.isInstanceOf[VFDTAttributes]) {
      val attribute = value._2.asInstanceOf[VFDTAttributes]

      //            //check if there is a spectator for this leaf
      //            if (leafsObserver.contains(attribute.leaf)) {
      //              leafClassTemp = leafsObserver.apply(attribute.leaf) //take the leaf
      // observer
      //            }
      //            else {
      //              leafClassTemp = new mutable.HashMap[String, mutable.HashMap[Long,
      //                AttributeObserver[Metrics]]]
      //            }

      if (leafsObserver.contains(attribute.leaf)) {
        //take the class observer
        attributesObserverTemp = leafsObserver.apply(attribute.leaf)
      }
      else {
        //if there is no observer for that leaf
        attributesObserverTemp = new mutable.HashMap[Long, AttributeObserver[Metrics]]()
      }
      //check if there is an attributeSpectator for this attribute, update metrics
      if (attributesObserverTemp.contains(value._1)) {
        attributesObserverTemp.apply(value._1).updateMetricsWithAttribute(attribute)
      }
      else {
        //if there is no attributeSpectator create one, nominal or numerical
        val tempSpectator = if (attribute.attributeType == AttributeType.Nominal) {
          new NominalAttributeObserver(2)
        }
        else {
          new NumericalAttributeObserver
        }
        tempSpectator.updateMetricsWithAttribute(attribute)

        attributesObserverTemp.put(value._1,
          tempSpectator.asInstanceOf[AttributeObserver[Metrics]])
      }
      //            leafClassTemp.put(attribute.clazz.toString, attributesSpectatorTemp)
      leafsObserver.put(attribute.leaf, attributesObserverTemp)
      //      println(leafsObserver)
    }
    //TODO:: if signal, calculate and feed the sub-model back to the iteration
    if (value._2.isInstanceOf[CalculateMetricsSignal]) {
      if (leafsObserver.contains(value._2.asInstanceOf[CalculateMetricsSignal].leaf)) {
        val leafToSplit = leafsObserver.apply(value._2.asInstanceOf[CalculateMetricsSignal].leaf)

        //[Long,HasMap[String,(#Yes,#No)]]
        for (attr <- leafToSplit) {
          val temp = attr._2.getSplitEvaluationMetric
          bestAttributesToSplit += ((attr._1, temp))
        }
        bestAttributesToSplit = bestAttributesToSplit sortWith ((x, y) => x._2._2 < y._2._2)
        println("------best Attribute: " + bestAttributesToSplit)
        var bestAttr: (Long, Double) = null
        var secondBestAttr: (Long, Double) = null
        if (bestAttributesToSplit.size > 0) {
          bestAttr = (bestAttributesToSplit(0)._1, bestAttributesToSplit(0)._2._2)
        }
        if (bestAttributesToSplit.size > 0) {
          secondBestAttr = (bestAttributesToSplit(1)._1, bestAttributesToSplit(1)._2._2)
        }
        out.collect(EvaluationMetric(bestAttr, secondBestAttr, 0.0))
      }
    }
  }
}

