package org.apache.flink.streaming.examples.wordcount

import java.lang.Iterable

import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.examples.java.ml.util.LinearRegressionData
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.examples.wordcount.LinearRegression.{IterationSelector, UpdateAccumulator, SubUpdate}
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class LinearRegressionScala {

def main (args: Array[String]){
  if(!parseParameters(args)) {
    return
  }

  // set up execution environment

  val env = ExecutionEnvironment.getExecutionEnvironment

  //FIXME
  val typedList = LinearRegressionData.DATA.map()

  env.setParallelism(1)

  // get input x data from elements
  val data = env.fromCollection(typedList)

  val iteration = data.iterate(5000)

  val step = iteration
      // compute a single step using every sample
      .flatMap(new SubUpdate)
      // sum up all the steps
      .flatMap(UpdateAccumulator)
      // average the steps and update all parameters
      .map(new Update)
      .shuffle()
      .split(IterationSelector)

  iteration.closeWith(step.select("iterate"))

  step.select("output").print()
  env.execute()
}

  // *************************************************************************
  //     DATA TYPES
  // *************************************************************************

  /**
   * A simple data sample, x means the input, and y means the target.
   */
  case class Data(var x: Double, var y: Double)  {
    override def toString: String = { return "(" + x + "|" + y + ")" }
  }

  /**
   * A set of parameters -- theta0, theta1.
   */
  case class Params(theta0: Double, theta1: Double) {
    def div(a: Int): Params = {
      Params(theta0 / a, theta1 / a)
    }
    override def toString: String = {return theta0 + " " + theta1}
  }
  // *************************************************************************
  //     USER FUNCTIONS
  // *************************************************************************

  /**
   * Compute a single BGD type update for every parameters.
   */

    class SubUpdate extends RichFlatMapFunction[(Boolean, Data, Params), (Boolean, Data, (Params, Int))]{
    private var parameter: Params = Params(0.0, 0.0)
    private var count: Int = 1

    override def flatMap(in: (Boolean, Data, Params), collector: Collector[(Boolean, Data, (Params, Int))]): Unit = {
      if (in._1) {
        val theta_0 = parameter.theta0 - 0.01 * ((parameter.theta0 + (parameter.theta1 * in._2.x)) - in._2.y)
        val theta_1 = parameter.theta1 - 0.01 * ((parameter.theta0 + (parameter.theta1 * in._2.x)) - in._2.y) * in._2.x

        //updated params
        //FIXME


        //data forward
        //FIXME

      }
      else
        {
          parameter = in._3
        }
    }
  }

    abstract class UpdateAccumulator extends FlatMapFunction[(Boolean, Data, (Params, Int)), (Boolean, Data, (Params, Int))]{

      //FIXME
      val value: [Boolean=true, Data, (Params, Int)] = [(0.0, 0.0), 0]

      override def flatMap(in: (Boolean, Data, (Params, Int)), collector: Collector[(Boolean, Data, (Params, Int))]) = {
        if (in._1){
          collector.collect(in);
        }
        else {
          //FIXME
          val val1: [Params, Int] = in._3
          val val2: [Params, Int] = value._3

          //FIXME
          val new_theta0: Double = val1._1.theta0 + val2._theta0
          val new_theta1: Double = val1._1.theta1 + val2._theta1
          val result: Params = (new_theta0, new_theta1)

          //FIXME
          collector.collect()

        }
      }
    }

  abstract class Update extends MapFunction[(Boolean, Data, (Params, Int)), (Boolean, Data, (Params, Int))] {
    override def map(in: (Boolean, Data, (Params, Int))): (Boolean, Data, (Params, Int)) = {
      if (in._1) {
          [true, in._2, Params]
      }
      else
        {
          [false, in._2, in._3._1.div(in._3._2)]
        }
    }
  }

  abstract class IterationSelector extends OutputSelector[Boolean, Data, Params] {
    val rnd: Random

    override def select(value: [Boolean, Data, Params]): Iterable[String] = {
      val output = new Array[]()
      if (rnd.nextInt(10) < 6) {
        output.add("output")
      }
      else {
        output.add("iterate")
      }
      output
    }
  }

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

    private var fileOutput: Boolean = false
    private var dataPath: String = null
    private var outputPath: String = null
    private var numIterations: Int = 10

  private def parseParameters(programArguments: Array[String]): Boolean = {
    if(programArguments.length > 0) {
      //parse input arguments
      fileOutput = true
      if (programArguments.length == 3) {
        dataPath = programArguments(0)
        outputPath = programArguments(1)
        numIterations = programArguments(2).toInt

        true
      } else {
        System.err.println("Usage: LinearRegression <data path> <result path> <num iterations>")

        false
      }
    } else {
      System.out.println("Executing Linear Regression example with default parameters and " +
        "built-in default data.")
      System.out.println("  Provide parameters to read input data from files.")
      System.out.println("  See the documentation for the correct format of input files.")
      System.out.println("  We provide a data generator to create synthetic input files for this " +
        "program.")
      System.out.println("  Usage: LinearRegression <data path> <result path> <num iterations>")

      true
    }
    }
}
