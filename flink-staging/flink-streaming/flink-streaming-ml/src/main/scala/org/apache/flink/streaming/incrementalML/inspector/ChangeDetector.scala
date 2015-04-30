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
package org.apache.flink.streaming.incrementalML.inspector

/** Base trait for an algorithm which detects concept drift on the input data.
  *
  * A [[ChangeDetector]] is used to detect concept changes on the input data to some output data.
  * Transformations might
  * be feature extractors, feature mappings, whitening or centralization just to name a few.
  *
  */
trait ChangeDetector {

  /** True in case there was a change detected
    *
    */
  var isChangedDetected: Boolean = false

  //TODO:: Decide whether this function should return a boolean value or Unit
  /** Adding another observation to the change detector
    *
    * Change detector's output is updated with the new data point
    *
    * @param inputPoint the new input point to change detector
    *
    * @return True if a change was detected
    */
  def input(inputPoint: Double): Boolean

  /** Resets the change detector.
    *
    */
  def reset(): Unit

  /** Copies the ChangeDetector instance
    *
    * @return Copy of the ChangeDetector instance
    */
  def copy(): ChangeDetector

}
