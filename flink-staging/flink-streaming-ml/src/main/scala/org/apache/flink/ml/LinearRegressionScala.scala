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

package org.apache.flink.ml

import java.lang.Iterable

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.util.Collector
import org.apache.flink.examples.java.ml.util.LinearRegressionData

import scala.util.Random
import scala.collection.JavaConversions._

object LinearRegressionScala {

def main (args: Array[String]){
  // set up execution environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val typedList = LinearRegressionData.DATA
    .map(pair => Data(pair(0).toString.toDouble, pair(0).toString.toDouble))
    .map(x => (true, x, Params(0.0, 0.0)))
  val dataList = typedList :+ (false, Data(0.0, 0.0), Params(0.0, 0.0))

  //FIXME
  env.setParallelism(1)

//   get input x data from elements
  val data = env.fromCollection(typedList)

  val iteration = data.iterate { data =>
    val newData : SplitStream[(Boolean, Data, Params)] = data
      // compute a single step using every sample
      .flatMap(new SubUpdate)
      // sum up all the steps
      .flatMap(new UpdateAccumulator)
      // average the steps and update all parameters
      .map(new Update)
      .shuffle
      .split(new IterationSelector)
    (newData.select("output"), newData.select("output"))
  }

  iteration print

  env execute
}

  // *************************************************************************
  //     DATA TYPES
  // *************************************************************************

  /**
   * A simple data sample, x means the input, and y means the target.
   */
  case class Data(var x: Double, var y: Double){
  }

  /**
   * A set of parameters -- theta0, theta1.
   */
  case class Params(theta0: Double, theta1: Double) {
    def div(a: Int): Params = {
      Params(theta0 / a, theta1 / a)
    }
  }
  // *************************************************************************
  //     USER FUNCTIONS
  // *************************************************************************

  /**
   * Compute a single BGD type update for every parameters.
   */

  class SubUpdate extends RichFlatMapFunction[(Boolean, Data, Params), (Boolean, Data, (Params, Int))]{
    private var parameter: Params = Params(0.0, 0.0)
    private val count: Int = 1

    override def flatMap(in: (Boolean, Data, Params), collector: Collector[(Boolean, Data, (Params, Int))]): Unit = {
      if (in._1) {
        val theta_0 = parameter.theta0 - 0.01 * ((parameter.theta0 + (parameter.theta1 * in._2.x)) - in._2.y)
        val theta_1 = parameter.theta1 - 0.01 * ((parameter.theta0 + (parameter.theta1 * in._2.x)) - in._2.y) * in._2.x

        //updated params
        collector.collect((false, Data(0.0, 0.0), (Params(theta_0, theta_1), count)))

        //data forward
        collector.collect((true, in._2, (Params(0.0, 0.0), 0)))
      } else {
        parameter = in._3
      }
    }
  }

  class UpdateAccumulator extends FlatMapFunction[(Boolean, Data, (Params, Int)), (Boolean, Data, (Params, Int))]{
    val value = (false, Data(0.0, 0.0), (Params(0, 0), 0))

    override def flatMap(in: (Boolean, Data, (Params, Int)), collector: Collector[(Boolean, Data, (Params, Int))]) = {
      if (in._1){
        collector.collect(in);
      }
      else {
        val val1 = in._3
        val val2 = value._3

        val new_theta0: Double = val1._1.theta0 + val2._1.theta0
        val new_theta1: Double = val1._1.theta1 + val2._1.theta1
        val result = Params(new_theta0, new_theta1)

        collector.collect((false, Data(0.0, 0.0), (result, val1._2 + val2._2)))

      }
    }
  }

  class Update extends MapFunction[(Boolean, Data, (Params, Int)), (Boolean, Data, Params)] {
    override def map(in: (Boolean, Data, (Params, Int))): (Boolean, Data, Params) = {
      if (in._1) {
        (true, in._2, Params(0.0, 0.0))
      } else {
        (false, in._2, in._3._1.div(in._3._2))
      }
    }
  }

  class IterationSelector extends OutputSelector[(Boolean, Data, Params)] {
    val rnd = new Random()

    override def select(value: (Boolean, Data, Params)): Iterable[String] = {
      if (rnd.nextInt(10) < 6) {
        List("output")
      } else {
        List("iterate")
      }
    }
  }
}
