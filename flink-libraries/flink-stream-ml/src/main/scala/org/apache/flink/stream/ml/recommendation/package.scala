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


package org.apache.flink.stream.ml

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.stream.ml.pipeline.{PredictDataStreamOperation, StreamPredictor}
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.streaming.api.scala._

import scala.language.implicitConversions

package object recommendation {

  implicit def streamify(als: ALS) : ALSStream = {new ALSStream(als)}

  class ALSStream(als : ALS) extends StreamPredictor[ALSStream]{
  }
  object ALSStream {
    /** Predict operation which calculates the matrix entry for the given indices  */
    implicit val predictRatingStream =
      new PredictDataStreamOperation[ALS, (Int, Int), (Int ,Int, Double)] {
        override def predictDataStream(
                                        instance: ALS,
                                        predictParameters: ParameterMap,
                                        input: DataStream[(Int, Int)])
        : DataStream[(Int, Int, Double)] = {

          instance.factorsOption match {
            case Some((userFactors, itemFactors)) => {

              val p = userFactors.collect.map(x => (x.id, x.factors)).toMap
              val q = itemFactors.collect.map(x => (x.id, x.factors)).toMap


              input.map(new RichMapFunction[(Int, Int), (Int, Int, Double)] {

                override def map(value: (Int, Int)): (Int, Int, Double) = {
                  (value._1, value._2, blas.ddot(10, p(value._1), 1, q(value._2), 1))
                }
              })
            }

            case None => throw new RuntimeException("The ALS model has not been fitted to data. " +
              "Prior to predicting values, it has to be trained on data.")
          }
        }
      }
  }

}
