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

package org.apache.flink.stream.ml.pipeline

import org.apache.flink.ml.common.{ParameterMap, WithParameters}
import org.apache.flink.stream.ml.common.StreamMLTools
import org.apache.flink.streaming.api.scala._

/** Predictor trait for Flink's pipeline operators.
  *
  * A [[StreamPredictor]] calculates predictions for testing data based on the model it learned
  * during
  * the fit operation (training phase). In order to do that, the implementing class has to provide
  * a [[org.apache.flink.ml.pipeline.FitOperation]] and a [[PredictDataStreamOperation]] implementation for the correct types.
  * The
  * implicit values should be put into the scope of the companion object of the implementing class
  * to make them retrievable for the Scala compiler.
  *
  * The pipeline mechanism has been inspired by scikit-learn
  *
  * @tparam Self Type of the implementing class
  */
trait StreamPredictor[Self] extends WithParameters {
  that: Self =>

  /** Predict testing data according the learned model. The implementing class has to provide
    * a corresponding implementation of [[PredictDataStreamOperation]] which contains the
    * prediction
    * logic.
    *
    * @param testing Testing data which shall be predicted
    * @param predictParameters Additional parameters for the prediction
    * @param predictor [[PredictDataStreamOperation]] which encapsulates the prediction logic
    * @tparam Testing Type of the testing data
    * @tparam Prediction Type of the prediction data
    * @return
    */
  def predict[Testing, Prediction](
      testing: DataStream[Testing],
      predictParameters: ParameterMap = ParameterMap.Empty)(implicit
      predictor: PredictDataStreamOperation[Self, Testing, Prediction])
    : DataStream[Prediction] = {
    StreamMLTools.registerFlinkMLTypes(testing.getExecutionEnvironment)
    predictor.predictDataStream(this, predictParameters, testing)
  }
}

object StreamPredictor {

}

/** Type class for the predict operation of [[StreamPredictor]]. This predict operation works on
  * DataSets.
  *
  * [[StreamPredictor]]s either have to implement this trait or the [[PredictOperation]] trait. The
  * implementation has to be made available as an implicit value or function in the scope of
  * their companion objects.
  *
  * The first type parameter is the type of the implementing [[StreamPredictor]] class so that the
  * Scala
  * compiler includes the companion object of this class in the search scope for the implicit
  * values.
  *
  * @tparam Self Type of [[StreamPredictor]] implementing class
  * @tparam Testing Type of testing data
  * @tparam Prediction Type of predicted data
  */
trait PredictDataStreamOperation[Self, Testing, Prediction] extends Serializable{

  /** Calculates the predictions for all elements in the [[DataSet]] input
    *
    * @param instance The Predictor instance that we will use to make the predictions
    * @param predictParameters The parameters for the prediction
    * @param input The DataSet containing the unlabeled examples
    * @return
    */
  def predictDataStream(
      instance: Self,
      predictParameters: ParameterMap,
      input: DataStream[Testing])
    : DataStream[Prediction]
}


