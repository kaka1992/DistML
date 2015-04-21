package com.intel.distml.app.mnist

import com.intel.distml.api.Model
import com.intel.distml.platform.{TrainingConf, TrainingHelper}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yunlong on 2/28/15.
 */
object MNIST {
  @throws(classOf[InterruptedException])
  def main(args: Array[String]) {

    val sparkMaster = args(0)
    val sparkHome = args(1)
    val sparkMem = args(2)
    val appJars = args(3)

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("Word2Vec")
      .set("spark.executor.memory", sparkMem)
      .set("spark.home", sparkHome)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setJars(Seq(appJars))

    val spark = new SparkContext(conf)
    Thread.sleep(3000)

    val trainingFile: String = "hdfs://dl-s3:9000/test/mnist_train_1"
    val rawLines = spark.textFile(trainingFile)

    val config: TrainingConf = new TrainingConf()
    val m: Model = new MNISTModel

    TrainingHelper.startTraining(spark, m, rawLines, config)
    System.out.println("digit recognition has ended!")
  }
}
