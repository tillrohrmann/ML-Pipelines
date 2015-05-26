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
package org.apache.flink.streaming.incrementalML.classification

import java.lang.Iterable
import java.util

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction}
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.incrementalML.attributeObserver.{AttributeObserver,
NominalAttributeObserver, NumericalAttributeObserver}
import org.apache.flink.streaming.incrementalML.classification.Metrics._
import org.apache.flink.streaming.incrementalML.classification.VeryFastDecisionTree._
import org.apache.flink.streaming.incrementalML.common.{Learner, Utils}
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 *
 * @param context
 */
class VeryFastDecisionTree(
  context: StreamExecutionEnvironment)
  extends Learner[LabeledVector, (Int, Metrics)]
  with Serializable {

  //TODO:: Check what other parameters need to be set
  def setMinNumberOfInstances(minInstances: Int): VeryFastDecisionTree = {
    parameters.add(MinNumberOfInstances, minInstances)
    this
  }

  def setVfdtDelta(delta: Double): VeryFastDecisionTree = {
    parameters.add(VfdtDelta, delta)
    this
  }

  def setVfdtTau(tau: Double): VeryFastDecisionTree = {
    parameters.add(VfdtTau, tau)
    this
  }

  def setNominalAttributes(noNominalAttrs: Map[Int, Int]): VeryFastDecisionTree = {
    parameters.add(NominalAttributes, noNominalAttrs)
    this
  }

  def setNumberOfClasses(noClasses: Int): VeryFastDecisionTree = {
    parameters.add(NumberOfClasses, noClasses)
    this
  }

  def setParallelism(parallelism: Int): VeryFastDecisionTree = {
    parameters.add(Parallelism, parallelism)
    this
  }

  override def fit(input: DataStream[LabeledVector], fitParameters: ParameterMap):
  DataStream[(Int, Metrics)] = {
    val resultingParameters = this.parameters ++ fitParameters

    val dataPointsStream: DataStream[Metrics] = input.map(dp => DataPoints(dp))

    var prequentialEvaluation: DataStream[(Int, Metrics)] = null

    val out = dataPointsStream.iterate[Metrics](10000)(dataPointsStream => {
      val (feedback, output, preqEvalStream) = iterationFunction(dataPointsStream,
        resultingParameters)
      prequentialEvaluation = preqEvalStream
      (feedback, output)
    })
    prequentialEvaluation
  }

  private def iterationFunction(dataPointsStream: DataStream[Metrics],
    resultingParameters: ParameterMap): (DataStream[Metrics],
    DataStream[Metrics], DataStream[(Int, Metrics)]) = {

    val mSAds: DataStream[(Int, Metrics)] = dataPointsStream.flatMap(new GlobalModelMapper(
      resultingParameters)).setParallelism(1)

    val attributes = mSAds.filter(new FilterFunction[(Int, Metrics)] {
      override def filter(value: (Int, Metrics)): Boolean = {
        return value._1 >= 0
      }
    }).setParallelism(1)

    val modelAndSignal = mSAds.filter(new FilterFunction[(Int, Metrics)] {
      override def filter(value: (Int, Metrics)): Boolean = {
        return (value._1 == -2) //metric or Signal
      }
    }).setParallelism(1)

    val prequentialEvaluationStream = mSAds.filter(new FilterFunction[(Int, Metrics)] {
      override def filter(value: (Int, Metrics)): Boolean = {
        return (value._1 == -3) //InstanceClassification
      }
    }).setParallelism(1)

    val splitDs = attributes.groupBy(0).merge(modelAndSignal.broadcast)
      .flatMap(new PartialVFDTMetricsMapper(context)).setParallelism(resultingParameters.
      apply(Parallelism)).split(new OutputSelector[Metrics] {
      override def select(value: Metrics): Iterable[String] = {
        val output = new util.ArrayList[String]()

        value match {
          case _: EvaluationMetric =>
            output.add("feedback")
          case _ =>
            output.add("output")
        }
        output
      }
    })

    val feedback: DataStream[Metrics] = splitDs.select("feedback")
    val output: DataStream[Metrics] = splitDs.select("output")
    (feedback, output, prequentialEvaluationStream)
  }
}

object VeryFastDecisionTree {

  def apply(context: StreamExecutionEnvironment): VeryFastDecisionTree = {
    new VeryFastDecisionTree(context)
  }

  /** Minimum number of instances seen, before deciding the new splitting feature.
    *
    */
  case object MinNumberOfInstances extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(200)
  }

  /** Hoeffding Bound tau parameter
    *
    */
  case object VfdtTau extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.05)
  }

  /** Hoeffding Bound delta parameter
    *
    */
  case object VfdtDelta extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.0000001)
  }

  /** Map that specifies which attributes are Nominal and how many possible values they will have
    *
    */
  case object NominalAttributes extends Parameter[Map[Int, Int]] {
    override val defaultValue: Option[Map[Int, Int]] = None
  }

  /**
   * Specifies the number of classes that the problem to be solved will have
   */
  case object NumberOfClasses extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(2)
  }

  /**
   * Specifies the number of parallel instances for the local statistics
   */
  case object Parallelism extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(2)
  }

}


/** This Flat Mapper can take as input any value of  Metrics type.
  * Specifically the input values can be:
  * inType1: [[DataPoints]] -> Is a wrapper for a [[LabeledVector]] value
  * inType2: [[EvaluationMetric]] -> Contains, per node, at most two attributes that will give the
  * maximum Information Gain.
  *
  * This mapper emits tuples of (attributeId,valueToBeSent), where the identifier is used for
  * filtering the different types of [[Metrics]] that the valueToBeSent can be.
  *
  * The (identifier,valueToBeSent) can have any of the following values
  * outType1: (Int,VFDTAttributes) -> when an attribute has been emitted
  * outType2: (-2,CalculateMetricsSignal) -> When a signal for splitting a leaf is sent
  *
  */
class GlobalModelMapper(resultingParameters: ParameterMap)
  extends FlatMapFunction[Metrics, (Int, Metrics)] {

  //Create the root of the DecisionTreeModel
  val VFDT = DecisionTreeModel
  //counterPerLeaf -> (leafId,(#0,#1,#2))
  var counterPerLeaf = mutable.HashMap[Int, List[Int]]((0,
    List.fill(resultingParameters.apply(NumberOfClasses))(0)))
  var metricsFromLocalProcessors = mutable.HashMap[Int, mutable.Map[Int, (Int, List[(Int,
    (Double, List[Double]))])]]()
  VFDT.createRootOfTheTree

  override def flatMap(value: Metrics, out: Collector[(Int, Metrics)]): Unit = {
    var leafId = 0

    value match {
      case newDataPoint: DataPoints => {

        val featuresVector = newDataPoint.getFeatures

        //-------------------------------Prequential Evaluation-------------------------------
        //classify data point first
        leafId = VFDT.classifyDataPointToLeaf(featuresVector)
        val label = VFDT.getNodeLabel(leafId)
        out.collect((-3, InstanceClassification(label, newDataPoint.getLabel)))
        //-------------------------------end Prequential Evaluation-------------------------------

        val temp = counterPerLeaf.getOrElseUpdate(leafId, List.fill(
          resultingParameters.apply(NumberOfClasses))(0))

        //update metrics for leaf
        val t = temp(newDataPoint.getLabel.toInt) + 1
        counterPerLeaf.update(leafId, temp.updated(newDataPoint.getLabel.toInt, t))

//        newDataPoint.getLabel match {
//          case 0.0 =>
//            val t = temp(0) + 1
//            counterPerLeaf.update(leafId, temp.updated(0, t))
//          case 1.0 =>
//            val t = temp(1) + 1
//            counterPerLeaf.update(leafId, temp.updated(1, t))
//          case 2.0 =>
//            val t = temp(2) + 1
//            counterPerLeaf.update(leafId, temp.updated(2, t))
//          case _ =>
//            throw new RuntimeException(s"I am sorry there was some problem with that " +
//              s"class label:" + s" ${newDataPoint.getLabel}")
//        }

        //------------------------------------majority vote------------------------------------
        val tempList = counterPerLeaf(leafId).view.zipWithIndex //(value,index)
        val leafLabel = tempList maxBy (_._1)

        VFDT.setNodeLabel(leafId, leafLabel._2)
        //------------------------------------end majority vote------------------------------------

        //TODO:: change this piece of code
        val nominal = resultingParameters.get(NominalAttributes)
        nominal match {
          case None => {
            for (i <- 0 until featuresVector.size) {
              //emit numerical attribute
              VFDT.getNodeExcludingAttributes(leafId) match {
                case None => {
                  out.collect((i, VFDTAttributes(i, featuresVector(i), newDataPoint.getLabel, -1,
                    leafId, AttributeType.Numerical)))
                }
                case _ => {
                  //emit it only if the attribute is not in the excluded ones for the specific leaf
                  if (!VFDT.getNodeExcludingAttributes(leafId).get.contains(i)) {
                    out.collect((i, VFDTAttributes(i, featuresVector(i), newDataPoint.getLabel, -1,
                      leafId, AttributeType.Numerical)))
                  }
                }
              }
            }
          } //TODO:: correct this piece of code -> check if attribute is excluded
          case _ => {
            for (i <- 0 until featuresVector.size) {
              nominal.get.getOrElse(i, None) match {
                case nOfValue: Int => {
                  //emit Nominal attribute
                  out.collect((i, VFDTAttributes(i, featuresVector(i), newDataPoint.getLabel,
                    nOfValue, leafId, AttributeType.Nominal)))
                }
                case None => {
                  //emit numerical attribute
                  out.collect((i, VFDTAttributes(i, featuresVector(i), newDataPoint.getLabel, -1,
                    leafId, AttributeType.Numerical)))
                }
              }
            }
          }
        }
        //----------------------till here----------------------------------------------------------

        counterPerLeaf.getOrElse(leafId, None) match {
          case leafMetrics: List[Int] => {
            //if we have seen at least MinNumberOfInstances and are not all of the same class

            if ((leafMetrics.sum % resultingParameters.apply(MinNumberOfInstances) == 0) &&
              (leafMetrics.count(_ == 0) != leafMetrics.size - 1)) {

              val temp = CalculateMetricsSignal(leafId, leafMetrics.sum, false)
              out.collect((-2, temp))
            }
          }
          case None =>
            throw new RuntimeException(s"leaf:$leafId doesn't exist")
        }

      }
      case evaluationMetric: EvaluationMetric => {
        //Aggregate metrics and update global model.

        //if node is still a leaf and has not been split yet
        if (VFDT.nodeIsLeaf(evaluationMetric.leafId)) {
          // (leafId, Map(SignalId,(Counter, List((Attr,ProposedValues)))))
          val temp = metricsFromLocalProcessors.getOrElse(evaluationMetric.leafId, None)
          temp match {
            case None => {
              metricsFromLocalProcessors.put(evaluationMetric.leafId, mutable.Map(
                (evaluationMetric.signalIdNumber, (1, evaluationMetric.proposedValues))))
            } //(SignalId,(Counter,List(Attr,(entropy,proposedValues))))
            case t: mutable.HashMap[Int, (Int, List[(Int, (Double, List[Double]))])] => {
              t.getOrElse(evaluationMetric.signalIdNumber, None) match {
                case None => {
                  val tempHashMap = t + ((evaluationMetric.signalIdNumber, (1, evaluationMetric
                    .proposedValues)))
                  metricsFromLocalProcessors.update(evaluationMetric.leafId, tempHashMap)
                }
                case pv: (Int, List[(Int, (Double, List[Double]))]) => {
                  val tempPV = (pv._1 + 1, pv._2 ::: evaluationMetric.proposedValues)
                  t.update(evaluationMetric.signalIdNumber, tempPV)
                  metricsFromLocalProcessors.update(evaluationMetric.leafId, t)

                  // if all metrics from local processors have been received, then check for
                  // the best attribute to split.
                  if (tempPV._1 == resultingParameters.apply(Parallelism)) {

                    var bestValuesToSplit = tempPV._2
                    bestValuesToSplit = bestValuesToSplit sortWith ((x, y) => x._2._1 < y._2._1)

                    val nonSplitEntro = nonSplittingEntropy(counterPerLeaf.get(evaluationMetric
                      .leafId).get)

                    val bestInfoGain = nonSplitEntro - bestValuesToSplit(0)._2._1
                    val secondBestInfoGain = nonSplitEntro - bestValuesToSplit(1)._2._1

//                    println(s"bestValue: ${evaluationMetric.proposedValues(0)._2._1}, " +
//                      s"secondBestValue" +
//                      s": ${evaluationMetric.proposedValues(1)._2._1}, bestInfoGain: " +
//                      s"$bestInfoGain, secondBestInfoGain: $secondBestInfoGain, bestInfoGain-" +
//                      s"secondBestInfoGain: ${bestInfoGain - secondBestInfoGain}, " +
//                      s"nonSplitEntro: $nonSplitEntro")

                    //todo:: this hoeffding bound should be calculated with the number of
                    // instances when the signal was send?
                    val hoeffdingBoundVariable = hoeffdingBound(counterPerLeaf.get
                      (evaluationMetric.leafId).get)

                    if (((bestInfoGain - secondBestInfoGain > hoeffdingBoundVariable) &&
                      bestInfoGain >= nonSplitEntro) || (
                      (bestInfoGain - secondBestInfoGain < hoeffdingBoundVariable) && (
                        hoeffdingBoundVariable < resultingParameters.get(VfdtTau).get))) {

                      val nominal = resultingParameters.get(NominalAttributes)
                      nominal match {
                        case None => {
                          //if there are no Nominal attributes
                          VFDT.growTree(evaluationMetric.leafId,
                            evaluationMetric.proposedValues(0)._1, AttributeType.Numerical,
                            evaluationMetric.proposedValues(0)._2._2,
                            evaluationMetric.proposedValues(0)._2._1)

                          //garbage collection
                          counterPerLeaf-=(leafId)
                          metricsFromLocalProcessors -= (evaluationMetric.leafId)
                          out.collect((-2, CalculateMetricsSignal(leafId, 0, true)))
                        }
                        case _ => {
                          nominal.get.getOrElse(evaluationMetric.proposedValues(0)._1, None) match {
                            case None => {
                              VFDT.growTree(evaluationMetric.leafId, evaluationMetric
                                .proposedValues(0)._1,
                                AttributeType.Numerical, evaluationMetric.proposedValues(0)._2._2,
                                evaluationMetric.proposedValues(0)._2._1)
                              //garbage collect the counter for the grown leaf and the observers
                              // in the local statistics
                              counterPerLeaf-=(leafId)
                              metricsFromLocalProcessors -= (evaluationMetric.leafId)
                              out.collect((-2, CalculateMetricsSignal(leafId, 0, true)))
                            }
                            case x: Int => {
                              VFDT.growTree(evaluationMetric.leafId, evaluationMetric
                                .proposedValues(0)._1,
                                AttributeType.Nominal, evaluationMetric.proposedValues(0)._2._2,
                                evaluationMetric.proposedValues(0)._2._1)
                              //garbage collect the counter for the grown leaf and the observers
                              // in the local statistics
                              counterPerLeaf-=(leafId)
                              metricsFromLocalProcessors -= (evaluationMetric.leafId)
                              out.collect((-2, CalculateMetricsSignal(leafId, 0, true)))
                            }
                          }
                        }
                      }
                      println(s"------------------${VFDT.getDecisionTreeSize}------------------")
                      println(s"---VFDT:$VFDT")
                      //        val jsonVFDT = Utils.createJSON_VFDT(VFDT.decisionTree)
                      //        System.err.println(jsonVFDT)
                      println(s"---counterPerLeaf: $counterPerLeaf\n")
                    }
                    //garbage collection
                    metricsFromLocalProcessors.getOrElse(leafId,None) match {
                      case None =>
                      case _ =>
                        t -= (evaluationMetric.signalIdNumber)
                    }
                  }
                }
              }
            }
          }
        }
      }
      //-------------------check signals ---------------------------------------------
      case _ =>
        throw new RuntimeException(s"- WTF is that ${value.getClass}")
    }

  }

  private def hoeffdingBound(counterPerClass: List[Int]): Double = {
    val R_square = math.pow(Utils.logBase2(resultingParameters.get(NumberOfClasses).get), 2.0)
    val delta = resultingParameters.get(VfdtDelta).get

    val n = counterPerClass.sum

    val hoeffdingBound = math.sqrt((R_square * math.log(1.0 / delta)) / (2.0 * n))
    //    println(s"---hoeffding bound: $hoeffdingBound")
    hoeffdingBound
  }

  private def nonSplittingEntropy(leafMetrics: List[Int]): Double = {
    //P(class1)Entropy(class1) + P(class2)Entropy(class2) + ...
    val total = leafMetrics.sum.toDouble
    var entropy = 0.0

    leafMetrics.foreach(classMetric => {
      if (classMetric != 0) {
        entropy -= (classMetric / total) * Utils.logBase2(classMetric / total)
      }
    })
    entropy
  }

}

/** This Flat Mapper can take as input a tuple2 (Int,Metrics), which is either broadcasted or
  * groupedByKey from the [[GlobalModelMapper]].
  *
  * inType1: (Int,VFDTAttributes) -> when an attribute is received. It updates the metrics for
  * the specific attribute.
  * inType2: (-2,CalculateMetricsSignal) -> When a signal for splitting a leaf is received. It
  * calculates which of the attributes is a better candidate for a split in that leaf.
  *
  * This mapper emits values of type [[EvaluationMetric]], which extends [[Metrics]].
  *
  */
class PartialVFDTMetricsMapper(context: StreamExecutionEnvironment)
  extends FlatMapFunction[(Int, Metrics), Metrics] {

  //[LeafId,HashMap[AttributeId,AttributeObserver]]
  val leafsObserver = new mutable.HashMap[Int, mutable.HashMap
    [Int, AttributeObserver[Metrics]]]()

  var attributesObserverTemp = mutable.HashMap[Int, AttributeObserver[Metrics]]()

  override def flatMap(value: (Int, Metrics), out: Collector[Metrics]): Unit = {


    value._2 match {
      case attribute: VFDTAttributes => {
        //take the class observer, else if there is no observer for that leaf
        leafsObserver.getOrElseUpdate(attribute.leaf, {
          new mutable.HashMap[Int, AttributeObserver[Metrics]]()
        })

        //check if there is an attributeObserver for this attribute, update metrics
        //if there is no attributeObserver create one, nominal or numerical
        attributesObserverTemp.getOrElseUpdate(value._1, {
          if (attribute.attributeType == AttributeType.Nominal) {
            new NominalAttributeObserver(attribute.nOfDifferentValues)
          }
          else {
            new NumericalAttributeObserver
          }
        }).updateMetricsWithAttribute(attribute)

        leafsObserver.put(attribute.leaf, attributesObserverTemp)
        //        println(leafsObserver)
      }

      case calcMetricsSignal: CalculateMetricsSignal => {

        leafsObserver.getOrElse(calcMetricsSignal.leaf, None) match {

          case leafToSplit: mutable.HashMap[Int, AttributeObserver[Metrics]] => {

            //[attributeId,(entropy,ListOfSplittingValues)]
            var bestAttributesToSplit = mutable.MutableList[(Int, (Double, List[Double]))]()

            if (!calcMetricsSignal.deleteObserver) {
              for (attr <- leafToSplit) {
                val temp = attr._2.getSplitEvaluationMetric
                bestAttributesToSplit += ((attr._1, temp))
              }

              bestAttributesToSplit = bestAttributesToSplit sortWith ((x, y) => x._2._1 < y._2._1)
              var bestAttr: (Int, (Double, List[Double])) = null
              var secondBestAttr: (Int, (Double, List[Double])) = null
              if (bestAttributesToSplit.size > 0) {
                bestAttr = bestAttributesToSplit(0)
              }
              if (bestAttributesToSplit.size > 1) {
                secondBestAttr = bestAttributesToSplit(1)
              }
              val t = EvaluationMetric(List(bestAttr, secondBestAttr), calcMetricsSignal.leaf,
                calcMetricsSignal.signalId)
              out.collect(t)
            }
            else {
              leafsObserver.-(calcMetricsSignal.leaf) //delete observer of this leaf
            }
          }
          case None =>
        }
      }
      case _ =>
        throw new RuntimeException("- WTF is that, that you're " +
          "sending in the  PartialVFDTMetricsMapper" + value.getClass.toString)
    }
  }
}
