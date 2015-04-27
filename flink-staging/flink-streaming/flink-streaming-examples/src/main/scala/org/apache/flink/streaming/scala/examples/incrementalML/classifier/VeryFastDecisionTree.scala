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
package org.apache.flink.streaming.scala.examples.incrementalML.classifier

import java.lang.Iterable
import java.util

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction}
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.ml.common.{ParameterMap, LabeledVector, Parameter}
import org.apache.flink.ml.math.DenseVector
import VeryFastDecisionTree._
import org.apache.flink.util.Collector

/**
 * O Job graph pou tha exei o algorithmos exei ulopoihthei (iterations and merged streams etc)
 * Dummy times pernane stous operators
 *
 * @param context
 */
class VeryFastDecisionTree(
  context: StreamExecutionEnvironment)
  extends Learner[LabeledVector, Metrics]
  with Serializable {

  //TODO:: Check what other parameters need to be set
  def setMinNumberOfInstances(minInstances: Int): VeryFastDecisionTree = {
    parameters.add(minNumberOfInstances, minInstances)
    this
  }

  def setThreshold(thresh: Double): VeryFastDecisionTree = {
    parameters.add(threshold, thresh)
    this
  }

  override def fit(input: DataStream[LabeledVector], fitParameters: ParameterMap):
  DataStream[Metrics] = {
    val resultingParameters = this.parameters ++ fitParameters

    val dataPointsStream: DataStream[Metrics] = input.map(dp => DataPoints(dp))
    val out = dataPointsStream.iterate[Metrics](dataPointsStream => iterationFunction
      (dataPointsStream))
    out
  }

  private def iterationFunction(dataPointsStream: DataStream[Metrics]): (DataStream[Metrics],
    DataStream[Metrics]) = {

    val mSAds = dataPointsStream.setParallelism(1).flatMap(
      new FlatMapFunction[Metrics, (Long, Metrics)] {

        override def flatMap(value: Metrics, out: Collector[(Long, Metrics)]): Unit = {
          var counter = 0
          //if a data point is received
          if (value.isInstanceOf[DataPoints]) {
            //TODO:: 1. classify data point first
            //TODO:: 2. number of instances seen till now
            val tempVector = value.asInstanceOf[DataPoints].getVector
            for (i <- 0 until tempVector.size) {
              out.collect((i + 1, VFDTAttributes(i, tempVector(i), 1)))
              if (i == 2) {
                out.collect((-2, Signal(counter)))
                counter += 1
              }
            }
          } //if a sub-model is received
          else if (value.isInstanceOf[VFDTModel]) {
            //TODO:: Aggregate models and broadcast global model
            out.collect((-1, value))
          }
          else {
            throw new RuntimeException("--------------------WTF is that, that you're " +
              "sending me ??? x-( :" + value.getClass.toString)
          }
        }
      })
    //TODO:: Decide which values will declare if it is a Model or a Signal
    val attributes = mSAds.filter(new FilterFunction[(Long, Metrics)] {
      override def filter(value: (Long, Metrics)): Boolean = {
        return value._1 > 0
      }
    })

    val modelAndSignal = mSAds.filter(new FilterFunction[(Long, Metrics)] {
      override def filter(value: (Long, Metrics)): Boolean = {
        return (value._1 == -1 || value._1 == -2)
      }
    })

    val splitDs = attributes.groupBy(0).merge(modelAndSignal.broadcast).flatMap(new
        FlatMapFunction[(Long, Metrics), Metrics] {

      override def flatMap(value: (Long, Metrics), out: Collector[Metrics]): Unit = {
        //TODO:: if attribute just do update the metrics
        //TODO:: if signal, calculate and feed the sub-model back to the iteration
        if (value._2.isInstanceOf[Signal]) {
          out.collect(VFDTModel(-100))
        }
      }
    }).split(new OutputSelector[Metrics] {
      override def select(value: Metrics): Iterable[String] = {
        val output = new util.ArrayList[String]()

        if (value.isInstanceOf[VFDTModel]) {
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
  case object minNumberOfInstances extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(200)
  }

  /** Hoeffding Bound threshold
    *
    */
  case object threshold extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(1.0)
  }

  def apply(context: StreamExecutionEnvironment): VeryFastDecisionTree = {
    new VeryFastDecisionTree(context)
  }

  val data: Seq[LabeledVector] = List(LabeledVector(1.0, DenseVector(Array(2104.00, 3.00, -7, 0))),
    LabeledVector(1.0, DenseVector(Array(1600.00, 4.00, -12, 0.7)))
    //    LabeledVector(1.0, DenseVector(Array(2400.00, 3.00, -3, 0)))
  )

  def main(args: Array[String]) {
    val con = StreamExecutionEnvironment.getExecutionEnvironment
    val vfdt = VeryFastDecisionTree(con)
    vfdt.fit(con.fromCollection(data))
    con.execute()
  }
}
