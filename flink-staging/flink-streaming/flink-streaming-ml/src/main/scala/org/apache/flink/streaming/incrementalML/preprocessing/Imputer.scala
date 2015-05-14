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
package org.apache.flink.streaming.incrementalML.preprocessing

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.incrementalML.common.Transformer
import org.apache.flink.streaming.incrementalML.preprocessing.ImputationStrategy.ImputationStrategy
import org.apache.flink.streaming.incrementalML.preprocessing.Imputer.Strategy

/** Cleans the observations that have missing values in any of the features.
  * By default for [[Imputer]] observations with missing values are deleted form the data set
  *
  * This transformer takes a [[LabeledVector]] of values and maps it to a [[LabeledVector]].
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.streaming.incrementalML.common.Learner]] implementations which
  * expect an input of [[LabeledVector]].
  *
  * @example
  * {{{
  *              val trainingDS: DataStream[LabeledVector] = env.fromCollection(data)
  *              val transformer = Imputer.setStrategy(ImputationStrategy.Deletion)
  *
  *              transformer.transform(trainingDS)
  * }}}
  *
  * - [[Imputer.Strategy]]: The Imputation Strategy to be followed; by default equal to
  * ImputationStrategy.Deletion
  */
class Imputer
  extends Transformer[LabeledVector, LabeledVector]
  with Serializable {

  /** Sets the target mean of the transformed data
    *
    * @param strategy the user-specified Imputation Strategy.
    * @return the Imputer instance with its Imputation Strategy set to the user-specified value
    */
  def setStrategy(strategy: ImputationStrategy): Imputer = {
    parameters.add(Strategy, strategy)
    this
  }

  override def transform(input: DataStream[LabeledVector], transformParameters: ParameterMap):
  DataStream[LabeledVector] = {

    val resultingParameters = this.parameters ++ parameters

    resultingParameters(Strategy) match {
      case ImputationStrategy.Deletion =>
        imputationWithDeletion(input)
      case _ =>
        input
    }
  }

  def imputationWithDeletion(inputStream: DataStream[LabeledVector]) : DataStream[LabeledVector] = {
    inputStream.filter(new FilterFunction[LabeledVector] {
      override def filter(value: LabeledVector): Boolean = {
        return value.vector.asBreeze.forall(elem => (!elem.isNaN))
      }
    })
  }

}

object Imputer {

  case object Strategy extends Parameter[ImputationStrategy] {
    override val defaultValue: Option[ImputationStrategy] = Some(ImputationStrategy.Deletion)
  }

  def apply(): Imputer = {
    new Imputer()
  }
}

object ImputationStrategy extends Enumeration with Serializable {
  type ImputationStrategy = Value
  val Deletion = Value
}