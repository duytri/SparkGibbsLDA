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
import scala.collection.mutable.ArrayBuffer
import main.scala.helper.TwoDimIntAcc
import main.scala.helper.OneDimIntAcc
import main.scala.helper.TwoDimIntAcc2
import main.scala.helper.OneDimDoubleAcc
import main.scala.connector.WordMap2File
import java.io.File
import main.scala.connector.Model2File
import org.apache.spark.sql.SparkSession

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
          println("Preparing...")

          //~ Create model ~
          val setFiles = sc.wholeTextFiles(params.directory + "/*").map { _._2 }.map(_.split("\n"))
          setFiles.cache()
          val M = setFiles.count().toInt // number of docs
          val vocab = setFiles.flatMap(x => x).distinct()
          val word2id = vocab.collect().zipWithIndex.toMap
          val V = word2id.size // number of vocabulary
          val bcW2I = sc.broadcast(word2id)
          val bcI2W = sc.broadcast(word2id.map(item => {
            item._2 -> item._1
          }))

          val bcK = sc.broadcast(params.K)
          val bcV = sc.broadcast(V)
          val bcAlpha = sc.broadcast(params.alpha)
          val bcBeta = sc.broadcast(params.beta)

          val setDocs = setFiles.map(file => {
            var ids = new ArrayBuffer[Int]
            file.foreach(word => {
              ids.append(bcW2I.value.get(word).get)
            })
            new Document(ids)
          }).zipWithIndex
          setDocs.cache()

          var nw = new TwoDimIntAcc(V, params.K)
          var nd = new TwoDimIntAcc(M, params.K)
          var nwsum = new OneDimIntAcc(params.K)
          var ndsum = new OneDimIntAcc(M)
          sc.register(nw, "Number of words assigned to topic")
          sc.register(nd, "Number of words in document assigned to topic")
          sc.register(nwsum, "Total number of words assigned to topic")
          sc.register(ndsum, "Total number of words in document")

          var z = new TwoDimIntAcc2(M)
          sc.register(z, "Topic assignment")

          setDocs.foreach {
            case (doc: Document, id: Long) => {
              val m = id.toInt // index of document
              val N = doc.wordIndexes.length // number of words in document
              //initilize for z
              z.initSecondDim(m, N)
              for (n <- 0 until N) {
                val topic = Math.floor(Math.random() * bcK.value).toInt // topic j
                z.setValue((m, n, topic))

                // number of instances of word assigned to topic j
                nw.add((doc.wordIndexes(n), topic, 1))
                // number of words in document m assigned to topic j
                nd.add((m, topic, 1))
                // total number of words assigned to topic j
                nwsum.add((topic, 1))
              }
              // total number of words in document m
              ndsum.add((m, N))
            }
          }

          val bcM = sc.broadcast(M)
          val bcNW = sc.broadcast(nw.value)
          val bcND = sc.broadcast(nd.value)
          val bcNWSUM = sc.broadcast(nwsum.value)
          val bcNDSUM = sc.broadcast(ndsum.value)
          val bcZ = sc.broadcast(z.value)

          var iterationData = setDocs.map(document => {
            (document, bcNW.value, bcND.value, bcNWSUM.value, bcZ.value)
          })
          println("Estimating...")
          for (iter <- 0 until params.niters) {
            iterationData = iterationData.map {
              case (document: (Document, Long), nw: Array[Array[Int]], nd: Array[Array[Int]], nwsum: Array[Int], z: Array[ArrayBuffer[Int]]) => {
                var nwNew: Array[Array[Int]] = null
                var ndNew: Array[Array[Int]] = null
                var nwsumNew: Array[Int] = null
                val m = document._2.toInt
                for (n <- 0 until document._1.wordIndexes.length) {
                  val w = document._1.wordIndexes(n)
                  var topic = z(m)(n)
                  // z_i = z[m][n]
                  // sample from p(z_i|z_-i, w)
                  val results = sampling(m, w, topic, bcV.value, bcK.value, bcAlpha.value, bcBeta.value, nw, nd, nwsum, bcNDSUM.value)
                  val topicNew = results._1
                  nwNew = results._2
                  ndNew = results._3
                  nwsumNew = results._4
                  z(m).update(n, topicNew)
                } // end for each word
                (document, nwNew, ndNew, nwsumNew, z)
              } // end for each document
            }
          }

          val results = iterationData.collect()

          var theta = computeTheta(M, params.K, params.alpha, nd.value, ndsum.value)
          var phi = computePhi(params.K, V, params.beta, nw.value, nwsum.value)
          //~~~~~~~~~~~ Writing results ~~~~~~~~~~~
          if (!params.output.equals("@")) {
            WordMap2File.writeWordMap(params.output + File.separator + params.wordMapFileName, word2id)
            Model2File.saveModel(params.output, params.modelname, params.alpha, params.beta, params.K, M, V, params.twords, params.niters - 1, setDocs.collect(), z.value, theta, phi, bcI2W.value)
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