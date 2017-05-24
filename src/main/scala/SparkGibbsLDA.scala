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
          val p = new Array[Double](params.K)

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
          sc.register(nw)
          sc.register(nd)
          sc.register(nwsum)
          sc.register(ndsum)

          var z = new TwoDimIntAcc2(M)
          sc.register(z)
          
          setDocs.foreach(doc => {
            val m = doc._2.toInt // index of document
            val N = doc._1.wordIndexes.length // number of words in document
            //initilize for z
            z.initSecondDim(m, N)
            for (n <- 0 until N) {
              val topic = Math.floor(Math.random() * bcK.value).toInt // topic j
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

          var theta = Array.ofDim[Double](M, params.K)
          var phi = Array.ofDim[Double](params.K, V)

          println("Number of tokens in this corpus: " + ndsum.value.reduce { _ + _ })

          println("Estimating...")

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
}