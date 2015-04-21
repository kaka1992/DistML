package com.intel.distml.app.lr

import java.io.{StringReader, BufferedReader}

import com.intel.distml.api.Model
import com.intel.distml.app.mnist.MNISTModel
import com.intel.distml.model.lr.LRModel
import com.intel.distml.model.lr.DataItem
import com.intel.distml.model.rosenblatt.Rosenblatt
import com.intel.distml.platform.{TrainingHelper, TrainingConf}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.Logger
import org.apache.log4j.Level



/**
 * Created by lq on 3/12/15.
 */
object LR{
  @throws(classOf[InterruptedException])
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkMaster = args(0)
    val sparkHome = args(1)
    val sparkMem = args(2)
    val appJars = args(3)

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("LR")
      .set("spark.executor.memory", sparkMem)
      .set("spark.home", sparkHome)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setJars(Seq(appJars))

    val spark = new SparkContext(conf)
    Thread.sleep(3000)

    val trainingFile: String ="/home/lq/data/lr1.txt"// "hdfs://dl-s3:9000/test/lr1.txt"
    val rawLines = spark.textFile(trainingFile)
    val dim=3
    val sampleRdd=rawLines

    val m: LRModel = new LRModel(dim)

    //val config = new TrainingConf();
    val config = new TrainingConf().iteration(2)
    config.miniBatchSize=100
    TrainingHelper.startTraining(spark, m, sampleRdd, config)
    m.show()

    spark.stop()
    System.out.println("===== Run Done ====")
  }
}
