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

import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.ml.math.Vector
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.incrementalML.common.Transformer

/** Cleans the observations that have missing values in any of the features.
  * By default for [[Imputer]] observations with missing values are deleted form the data s
et
  *
  * This transformer takes a [[Vector]] of values and maps it to a
  * scaled [[Vector]].
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.streaming.incrementalML.common.Learner]] implementations which
  * expect an input of [[Vector]].
  *
  * @example
  * {{{
  *             val trainingDS: DataStream[Vector] = env.fromCollection(data)
  *             val transformer = Imputer
  *
  *             transformer.transform(trainingDS)
  * }}}
  *
  * =Parameters=
  */
class Imputer
  extends Transformer[Vector, LabeledVector]
  with Serializable {

  override def transform(input: DataStream[Vector], transformParameters: ParameterMap):
  DataStream[LabeledVector] = {
    null
  }
}


object Imputer {


  def apply(): Imputer = {
    new Imputer()
  }
}