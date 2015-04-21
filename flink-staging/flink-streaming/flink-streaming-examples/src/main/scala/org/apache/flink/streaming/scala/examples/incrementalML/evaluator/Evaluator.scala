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
package org.apache.flink.streaming.scala.examples.incrementalML.evaluator

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.scala.examples.incrementalML.MLModel


/** Base trait for algorithm which evaluates the current model for the input data points
  *
  * A [[Evaluator]] implementation has to implement the method `evaluate`, which defines how
  * well the current model performs with the incoming data set.
  *
  * @tparam IN Type of incoming elements
  * @tparam OUT Type of outgoing elements
  */
trait Evaluator[IN, OUT] {

  /** Update the model that should be evaluated. As the model is updated by the learner the
    * evaluator should always use the up-to-date-model.
    *
    * @param inputModel
    *
    * @return True if a change was detected
    */
  def updateModel(inputModel: MLModel): Unit

  /** Evaluating model's accuracy with the input observations
    *
    * @param inputPoints The points to be used for the evaluation.
    *
    * @return The Prediction error for each data point or for a test set
    */
  def evaluate(inputPoints: DataStream[IN]): DataStream[OUT]

}
