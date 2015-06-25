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
package org.apache.flink.streaming.incrementalML.test.classifier

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.io.TypeSerializerOutputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.ml.common.{LabeledVector, ParameterMap}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.incrementalML.classification.VerticalHoeffdingTree
import org.apache.flink.streaming.incrementalML.common.StreamingMLUtils
import org.apache.flink.streaming.incrementalML.evaluator.PrequentialEvaluator
import org.apache.flink.streaming.incrementalML.inspector.PageHinkleyTest
import org.apache.flink.test.util.FlinkTestBase
import org.apache.flink.util.Collector
import org.scalatest.{FlatSpec, Matchers}

class VerticalHoeffdingTreeITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's Vertical Hoeffding Tree algorithm"

  it should "Create the classification VHT of the given data set" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val parameters = ParameterMap()
//    val nominalAttributes = Map(0 ->4, 2 ->4, 4 ->4, 6 ->4, 8 ->4)

    parameters.add(VerticalHoeffdingTree.MinNumberOfInstances, 200)
    parameters.add(VerticalHoeffdingTree.NumberOfClasses, 3)
    parameters.add(VerticalHoeffdingTree.Parallelism, 8)
//    parameters.add(VerticalHoeffdingTree.NominalAttributes, nominalAttributes)

//    parameters.add(VerticalHoeffdingTree.OnlyNominalAttributes,true)


    val dataPoints = env.readTextFile("/Users/fobeligi/workspace/master-thesis/dataSets/" +
      "Waveform-MOA/Waveform-10M.csv").map {
      line => {
        var featureList = Vector[Double]()
        val features = line.split(',')
        for (i <- 0 until features.size -1 ) {
          featureList = featureList :+ features(i).trim.toDouble
        }

        LabeledVector(features(features.size -1).trim.toDouble, DenseVector(featureList.toArray))

      }
    }

//    val dataPoints = StreamingMLUtils.readLibSVM(env,
//     "/Users/fobeligi/workspace/master-thesis/dataSets/pokerHands/poker.t", 10)

    //    val transformer = Imputer()
    val vhtLearner = VerticalHoeffdingTree(env)
    val evaluator = PrequentialEvaluator()

    //    val vhtChainedLearner = new ChainedLearner[LabeledVector, LabeledVector, (Int, Metrics)](
    //      transformer, vhtLearner)

    val streamToEvaluate = vhtLearner.fit(dataPoints, parameters)

    val evaluationStream = evaluator.evaluate(streamToEvaluate)

    evaluationStream.writeAsCsv("/Users/fobeligi/workspace/master-thesis/dataSets/" +
      "Waveform-MOA/results/waveform1_8-test.csv").setParallelism(1)

//    val changeDetector = PageHinkleyTest()

//    changeDetector.detectChange(evaluationStream.map(x => x._2)).flatMap(new
//        UnifiedStreamBatchMapper())

    env.execute()
  }
}

class UnifiedStreamBatchMapper
  extends FlatMapFunction[Boolean, Boolean] {

  def createSubmitBatchJob(): Unit = {
    val batchEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val csvR: DataSet[(Double, Int)] = batchEnvironment.readCsvFile("/Users/fobeligi/workspace/" +
      "master-thesis/dataSets/UnifiedBatchStream.csv")

    csvR.write(new TypeSerializerOutputFormat[(Double, Int)], "/Users/fobeligi/workspace/" +
      "master-thesis/dataSets/FlinkTmp/temp", FileSystem.WriteMode.OVERWRITE)

    batchEnvironment.execute()
  }

  override def flatMap(value: Boolean, out: Collector[Boolean]): Unit = {
    if (value) {
      System.err.println(value)
      createSubmitBatchJob();
    }
    else {
      out.collect(value)
    }
  }
}
