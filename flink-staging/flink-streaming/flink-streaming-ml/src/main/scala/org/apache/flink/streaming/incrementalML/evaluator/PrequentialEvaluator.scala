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
package org.apache.flink.streaming.incrementalML.evaluator

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.incrementalML.classification.Metrics.{InstanceClassification,
Metrics}

class PrequentialEvaluator
  extends Evaluator[(Int, Metrics), Double]
  with Serializable {

  var instancesClassified = 0.0
  var sumLossFunction = 0.0

  /** Evaluating model's accuracy with the input observations
    *
    * @param inputDataStream The points to be used for the evaluation.
    *
    * @return The Prediction error for each data point
    */
  override def evaluate(inputDataStream: DataStream[(Int, Metrics)]): DataStream[Double] = {
    inputDataStream.map {
      input => {
        val temp = input._2.asInstanceOf[InstanceClassification]
        instancesClassified += 1.0
        if (temp.label != temp.clazz) {
          sumLossFunction += 1.0
        }
        sumLossFunction / instancesClassified
      }
    }.setParallelism(1)
  }
}

object PrequentialEvaluator {
  def apply(): PrequentialEvaluator = {
    new PrequentialEvaluator()
  }
}