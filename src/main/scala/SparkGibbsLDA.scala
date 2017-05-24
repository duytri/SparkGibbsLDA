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
          val conf = new SparkConf().setAppName("SparkGibbsLDA").setMaster("local[8]")
          val sc = new SparkContext(conf)
          //~~~~~~~~~~~ Timer ~~~~~~~~~~~
          val startTime = System.currentTimeMillis()

          //~~~~~~~~~~~ Body ~~~~~~~~~~~
          //println("#################### DAY LA PHAN THAN CUA CHUONG TRINH ####################")
          println("Preparing...")
          //~ Create model ~
          val p = new OneDimDoubleAcc(params.K)
          sc.register(p, "probability array")

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
          val bcParams = sc.broadcast(params)

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
          sc.register(z)

          setDocs.foreach(doc => {
            val m = doc._2.toInt // index of document
            val N = doc._1.wordIndexes.length // number of words in document
            //initilize for z
            z.initSecondDim(m, N)
            for (n <- 0 until N) {
              val topic = Math.floor(Math.random() * bcParams.value.K).toInt // topic j
              z.setValue((m, n, topic))

              // number of instances of word assigned to topic j
              nw.add((doc._1.wordIndexes(n), topic, 1))
              // number of words in document m assigned to topic j
              nd.add((m, topic, 1))
              // total number of words assigned to topic j
              nwsum.add((topic, 1))
            }
            // total number of words in document m
            ndsum.add((m, N))
          })

          //println("Number of tokens in this corpus: " + ndsum.value.reduce { _ + _ })

          println("Estimating...")

          val Vbeta = sc.broadcast(V * params.beta)
          val Kalpha = sc.broadcast(params.K * params.alpha)
          for (iter <- 0 until params.niters) {
            // for all z_i
            setDocs.foreach(doc => {
              val m = doc._2.toInt // index of document
              for (n <- 0 until doc._1.wordIndexes.length) {
                // z_i = z[m][n]
                // sample from p(z_i|z_-i, w)
                val topic = {
                  // remove z_i from the count variable
                  var topic = z.value()(m)(n)
                  val w = doc._1.wordIndexes(n)

                  nw.subtract((w, topic, 1))
                  nd.subtract((m, topic, 1))
                  nwsum.subtract((topic, 1))
                  ndsum.subtract((m, 1))

                  //do multinominal sampling via cumulative method
                  for (k <- 0 until bcParams.value.K) {
                    p.set((k), (nw.value()(w)(k) + bcParams.value.beta) / (nwsum.value()(k) + Vbeta.value) *
                      (nd.value()(m)(k) + bcParams.value.alpha) / (ndsum.value()(m) + Kalpha.value))
                  }

                  // cumulate multinomial parameters
                  for (k <- 1 until bcParams.value.K) {
                    p.add((k, p.value()(k - 1)))
                  }

                  // scaled sample because of unnormalized p[]
                  val u = Math.random() * p.value()(bcParams.value.K - 1)

                  //sample topic w.r.t distribution p
                  topic = 0
                  while (topic < bcParams.value.K && p.value()(topic) <= u) {
                    topic += 1
                  }
                  if (topic == bcParams.value.K) topic -= 1

                  // add newly estimated z_i to count variables
                  nw.add((w, topic, 1))
                  nd.add((m, topic, 1))
                  nwsum.add((topic, 1))
                  ndsum.add((m, 1))

                  topic
                }
                z.setValue((m, n, topic))
              } // end for each word
            }) // end for each document
          } // end iterations	

          var theta = computeTheta(M, params.K, params.alpha, nd.value, ndsum.value)
          var phi = computePhi(params.K, V, params.beta, nw.value, nwsum.value)

          //~~~~~~~~~~~ Writing results ~~~~~~~~~~~
          if (!params.output.equals("@")) {
            WordMap2File.writeWordMap(params.output + File.separator + params.wordMapFileName, word2id)
            Model2File.saveModel(params.output, params.modelname, params.alpha, params.beta, params.K, M, V, params.twords, params.niters-1, setDocs.collect(), z.value, theta, phi, bcI2W.value)
          }

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