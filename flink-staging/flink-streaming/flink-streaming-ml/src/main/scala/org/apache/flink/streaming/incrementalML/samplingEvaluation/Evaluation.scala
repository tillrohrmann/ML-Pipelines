
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

package org.apache.flink.streaming.incrementalML.samplingEvaluation

import org.apache.flink.streaming.api.scala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.incrementalML.classification.VeryFastDecisionTree
import org.apache.flink.streaming.sampling.helpers.SamplingUtils

/**
 * Created by marthavk on 2015-06-01.
 */
class Evaluation {

  object ClassificationTest {
    def main(args: Array[String]) {
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      val rawData: scala.DataStream[String] = env.readTextFile(SamplingUtils.path + "small_dataset")
      val tree = VeryFastDecisionTree(env)
      rawData.print();
    }
  }

}
