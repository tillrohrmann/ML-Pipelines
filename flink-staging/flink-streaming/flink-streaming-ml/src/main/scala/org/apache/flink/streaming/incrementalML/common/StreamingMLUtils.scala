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
package org.apache.flink.streaming.incrementalML.common

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.streaming.api.scala._

/** Imported from Batch Flink-ML
  *
  * Convenience functions for machine learning tasks.
  *
  * This object contains convenience functions for machine learning tasks:
  *
  * - readLibSVM:
  * Reads a libSVM/SVMLight input file and returns a data stream of [[LabeledVector]].
  * The file format is specified [http://svmlight.joachims.org/ here].
  *
  * - writeLibSVM:
  * Writes a data stream of [[LabeledVector]] in libSVM/SVMLight format to disk. The file format
  * is specified [http://svmlight.joachims.org/ here].
  */

object StreamingMLUtils {

  /** Reads a file in libSVM/SVMLight format and converts the data into a data stream of
    * [[LabeledVector]]. The dimension of the [[LabeledVector]] is determined automatically.
    *
    * Since the libSVM/SVMLight format stores a vector in its sparse form, the [[LabeledVector]]
    * will also be instantiated with a [[SparseVector]].
    *
    * @param env [[StreamExecutionEnvironment]]
    * @param filePath Path to the input file
    * @param dimension Dimension of data points
    * @return [[StreamExecutionEnvironment]] of [[LabeledVector]] containing the information of
    *         the libSVM/SVMLight file
    */
  def readLibSVM(env: StreamExecutionEnvironment, filePath: String, dimension: Int):
  DataStream[LabeledVector] = {
    val labelCOODS = env.readTextFile(filePath).flatMap {
      line =>
        // remove all comments which start with a '#'
        val commentFreeLine = line.takeWhile(_ != '#').trim

        if (commentFreeLine.nonEmpty) {
          val splits = commentFreeLine.split(' ')
          val label = splits.head.toDouble
          val sparseFeatures = splits.tail
          val coos = sparseFeatures.map {
            str =>
              val pair = str.split(':')
              require(pair.length == 2, "Each feature entry has to have the form <feature>:<value>")

              // libSVM index is 1-based, but we expect it to be 0-based
              val index = pair(0).toInt - 1
              val value = pair(1).toDouble

              (index, value)
          }

          Some((label, coos))
        } else {
          None
        }
    }

    labelCOODS.map {
      new MapFunction[(Double, Array[(Int, Double)]), LabeledVector] {

        override def map(value: (Double, Array[(Int, Double)])): LabeledVector = {
          new LabeledVector(value._1, SparseVector.fromCOO(dimension, value._2))
        }
      }
    }
  }

  /** Writes a [[DataStream]] of [[LabeledVector]] to a file using the libSVM/SVMLight format.
    *
    * @param filePath Path to output file
    * @param labeledVectors [[DataStream]] of [[LabeledVector]] to write to disk
    * @return
    */
  def writeLibSVM(filePath: String, labeledVectors: DataStream[LabeledVector]):
  DataStream[String] = {
    val stringRepresentation = labeledVectors.map {
      labeledVector =>
        val vectorStr = labeledVector.vector.
          // remove zero entries
          filter(_._2 != 0).
          map { case (idx, value) => (idx + 1) + ":" + value }.
          mkString(" ")

        labeledVector.label + " " + vectorStr
    }
    stringRepresentation.writeAsText(filePath)
  }
}
