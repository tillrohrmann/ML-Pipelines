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

package org.apache.flink.streaming.ml

import java.lang.Iterable

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.util.Collector
import org.apache.flink.examples.java.ml.util.LinearRegressionData

import scala.util.Random
import scala.collection.JavaConversions._

object LinearRegressionScala {
  type MyEither[L, R] = Either[L, R] with Product with Serializable

def main (args: Array[String]){
  // set up execution environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val typedList = LinearRegressionData.DATA
    .map(pair => Data(pair(0).toString.toDouble, pair(0).toString.toDouble))
    .map(Left(_))
  val dataList = typedList :+ Right(Params(0.0, 0.0))

  val data = env.fromCollection(dataList)
    .map(x => x)
    .rebalance

  val iteration = data.iterate{data =>
    val updated = data.flatMap(new SubUpdate)
    .flatMap(new UpdateAccumulator).setParallelism(1)
    .map(new Update)
    .broadcast

    val connected = data.connect(updated)
      .map(x => x, x => x)
      .split(new IterationSelector)
    (connected.select("iterate"), connected.select("output"))
  }

  iteration print

  System.out.println(env.getExecutionPlan)
}

  // *************************************************************************
  //     DATA TYPES
  // *************************************************************************

  /**
   * A simple data sample, x means the input, and y means the target.
   */
  case class Data(var x: Double, var y: Double)

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

  class SubUpdate extends RichFlatMapFunction[MyEither[Data, Params],
    MyEither[Data, (Params, Int)]]{
    private var parameter: Params = Params(0.0, 0.0)
    private val count: Int = 1

    override def flatMap(in: MyEither[Data, Params],
                         collector: Collector[MyEither[Data, (Params, Int)]]): Unit = {
      in match {
        case Left(data) => {
        val theta_0 = parameter.theta0 - 0.01 *
          ((parameter.theta0 + (parameter.theta1 * data.x)) - data.y)
        val theta_1 = parameter.theta1 - 0.01 *
          ((parameter.theta0 + (parameter.theta1 * data.x)) - data.y) * data.x

        //updated params
        collector.collect(Right(Params(theta_0, theta_1), count))

        //data forward
        collector.collect(Left(data))
        }
        case Right(param) => {
          parameter = param
        }
      }
    }
  }

  class UpdateAccumulator extends FlatMapFunction[MyEither[Data, (Params, Int)],
    MyEither[Data, (Params, Int)]]{
    var value = (Params(0.0, 0.0), 0)

    override def flatMap(in: MyEither[Data, (Params, Int)],
                         collector: Collector[MyEither[Data, (Params, Int)]]) = {
      in match {
        case Left(data) => collector.collect(in)
        case Right(param) => {

          val new_theta0: Double = param._1.theta0 + value._1.theta0
          val new_theta1: Double = param._1.theta1 + value._1.theta1
          value = (Params(new_theta0, new_theta1), param._2 + value._2)

          collector.collect(Right(value))
        }
      }
    }
  }

  class Update extends MapFunction[MyEither[Data, (Params, Int)], MyEither[Data, Params]] {
    override def map(in: MyEither[Data, (Params, Int)] ): MyEither[Data, Params] = {
      in match {
        case Left(data) => Left(data)
        case Right(param) => Right(param._1.div(param._2))
      }
    }
  }

  class IterationSelector extends OutputSelector[MyEither[Data, Params]] {
    @transient
    var rnd : Random = null

    override def select(value: MyEither[Data, Params]): Iterable[String] = {
      if (rnd == null) {
        rnd = new Random()
      }
      if (rnd.nextInt(10) < 6) {
        List("output")
      } else {
        List("iterate")
      }
    }
  }
}
