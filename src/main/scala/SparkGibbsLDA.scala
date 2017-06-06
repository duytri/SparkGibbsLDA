package main.scala

import main.scala.obj.Document
import main.scala.helper.LDACmdOption
import main.java.commons.cli.MissingOptionException
import main.java.commons.cli.MissingArgumentException
import main.java.commons.cli.CommandLine
import main.java.commons.cli.UnrecognizedOptionException
import main.scala.obj.Parameter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import org.apache.spark.sql.SparkSession
import breeze.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import main.scala.obj.LDA
import scala.collection.mutable.ArrayBuffer

object SparkGibbsLDA {

  def main(args: Array[String]): Unit = {
    println("Current directory: " + System.getProperty("user.dir"))
    println("#################### Gibbs sampling LDA in Apache Spark ####################")
    try {
      var cmd = LDACmdOption.getArguments(args)
      if (cmd.hasOption("help")) {
        LDACmdOption.showHelp()
      } else {
        // set user parameters
        var params = new Parameter
        params.getParams(cmd)
        if (!params.checkRequirement) {
          println("ERROR!!! Phai nhap day du cac tham so: alpha, beta, directory, ntopics, niters")
          LDACmdOption.showHelp()
          return
        } else {
          //~~~~~~~~~~~ Spark ~~~~~~~~~~~
          val conf = new SparkConf().setAppName("SparkGibbsLDA").setMaster("local[*]")
          val spark = SparkSession.builder().config(conf).getOrCreate()
          val sc = spark.sparkContext
          //~~~~~~~~~~~ Timer ~~~~~~~~~~~
          val startTime = System.currentTimeMillis()

          //~~~~~~~~~~~ Body ~~~~~~~~~~~
          //println("#################### DAY LA PHAN THAN CUA CHUONG TRINH ####################")
          val dataFiles = sc.wholeTextFiles(params.directory + "/*").map { _._2 }.map(_.split("\n"))
          val vocab = dataFiles.flatMap(x => x).distinct()
          val word2id = vocab.collect().zipWithIndex.toMap
          val bcWord2Id = sc.broadcast(word2id)
          val data = dataFiles.map(file => {
            var ids = new ArrayBuffer[Double]
            file.foreach(word => {
              ids.append(bcWord2Id.value.get(word).get)
            })
            ids.toArray
          })
          val parsedData = data.map(Vectors.dense(_))
          // Index documents with unique IDs
          val corpus = parsedData.zipWithIndex.map(_.swap).cache()

          // Cluster the documents into three topics using LDA
          val ldaModel = new LDA().setK(params.K).run(corpus, params.niters)

          // Output topics. Each is a distribution over words (matching word count vectors)
          println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
          val topics = ldaModel.topicsMatrix
          for (topic <- Range(0, params.K)) {
            print("Topic " + topic + ":")
            for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
            println()
          }

          spark.stop()
          //~~~~~~~~~~~ Timer ~~~~~~~~~~~
          val duration = System.currentTimeMillis() - startTime
          val millis = (duration % 1000).toInt
          val seconds = ((duration / 1000) % 60).toInt
          val minutes = ((duration / (1000 * 60)) % 60).toInt
          val hours = ((duration / (1000 * 60 * 60)) % 24).toInt
          println("#################### Finished in " + hours + " hour(s) " + minutes + " minute(s) " + seconds + " second(s) and " + millis + " millisecond(s) ####################")
        }
      }
    } catch {
      case moe: MissingOptionException => {
        println("ERROR!!! Phai nhap day du cac tham so: alpha, beta, directory, datafile, ntopics, niters")
        LDACmdOption.showHelp()
      }
      case mae: MissingArgumentException => {
        mae.printStackTrace()
        println("ERROR!!! Thieu gia tri cua cac tham so.")
        LDACmdOption.showHelp()
      }
      case uoe: UnrecognizedOptionException => {
        uoe.printStackTrace()
        println("ERROR!!! Chuong trinh khong ho tro tham so ban da nhap.")
        LDACmdOption.showHelp()
      }
      case e: Throwable => e.printStackTrace()
    }
  }

  def sampling(m: Int, w: Int, topic: Int, V: Int, K: Int, alpha: Double, beta: Double, nw: Array[Array[Int]], nd: Array[Array[Int]], nwsum: Array[Int], ndsum: Array[Int]): (Int, Array[Array[Int]], Array[Array[Int]], Array[Int]) = {

    var p = Array.ofDim[Double](K)

    nw(w)(topic) -= 1
    nd(m)(topic) -= 1
    nwsum(topic) -= 1

    //do multinominal sampling via cumulative method
    for (k <- 0 until K) {
      p(k) = (nw(w)(k) + beta) / (nwsum(k) + V * beta) *
        (nd(m)(k) + alpha) / (ndsum(m) - 1 + K * alpha)
    }

    // cumulate multinomial parameters
    for (k <- 1 until K) {
      p(k) += p(k - 1)
    }

    // scaled sample because of unnormalized p[]
    val u = Math.random() * p(K - 1)

    //sample topic w.r.t distribution p
    var topicNew = 0
    while (topicNew < K && p(topicNew) <= u) {
      topicNew += 1
    }
    if (topicNew == K) topicNew -= 1
    /*for (topic <- 0 until trnModel.K) {
      if (trnModel.p(topic) > u) //sample topic w.r.t distribution p
        break
    }*/

    // add newly estimated z_i to count variables
    nw(w)(topicNew) += 1;
    nd(m)(topicNew) += 1;
    nwsum(topicNew) += 1;

    (topicNew, nw, nd, nwsum)
  }

  def computeTheta(M: Int, K: Int, alpha: Double, nd: Array[Array[Int]], ndsum: Array[Int]): Array[Array[Double]] = {
    var theta = Array.ofDim[Double](M, K)
    for (m <- 0 until M) {
      for (k <- 0 until K) {
        theta(m)(k) = (nd(m)(k) + alpha) / (ndsum(m) + K * alpha)
      }
    }
    theta
  }

  def computePhi(K: Int, V: Int, beta: Double, nw: Array[Array[Int]], nwsum: Array[Int]): Array[Array[Double]] = {
    var phi = Array.ofDim[Double](K, V)
    for (k <- 0 until K) {
      for (w <- 0 until V) {
        phi(k)(w) = (nw(w)(k) + beta) / (nwsum(k) + V * beta)
      }
    }
    phi
  }
}