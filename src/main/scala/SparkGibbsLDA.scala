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
import main.scala.helper.Utils
import main.scala.obj.LDAModel

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
          
          //~~~~~~~~~~~ Body ~~~~~~~~~~~
          //println("#################### DAY LA PHAN THAN CUA CHUONG TRINH ####################")
          /*val dataFiles = sc.wholeTextFiles(params.directory + "/*").map { _._2.trim }.map(_.split("\n"))
          val vocab = dataFiles.flatMap(x => x).distinct()
          val word2id = vocab.collect().zipWithIndex.toMap
          val bcWord2Id = sc.broadcast(word2id)
          val data = dataFiles.map(file => {
            var ids = new ArrayBuffer[Double]
            file.foreach(word => {
              ids.append(bcWord2Id.value.get(word).get)
            })
            ids.toArray
          })*/*/
          //val parsedData = data.map(Vectors.dense(_))
          // Index documents with unique IDs
          //val corpus = parsedData.zipWithIndex.map(_.swap).cache()

          // Load documents, and prepare them for LDA.
          val preprocessStart = System.nanoTime()
          val (corpus, vocabArray, actualNumTokens) = Utils.preprocess(sc, params.directory + "/*")
          corpus.cache()
          val actualCorpusSize = corpus.count()
          val actualVocabSize = vocabArray.length
          val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9

          println()
          println(s"Corpus summary:")
          println(s"\t Training set size: $actualCorpusSize documents")
          println(s"\t Vocabulary size: $actualVocabSize terms")
          println(s"\t Training set size: $actualNumTokens tokens")
          println(s"\t Preprocessing time: $preprocessElapsed sec")
          println()

          // Cluster the documents into three topics using LDA
          val lda = new LDA()
            .setK(params.K)
            .setAlpha(params.alpha)
            .setBeta(params.beta)
            .setMaxIterations(params.niters)

          val startTime = System.nanoTime()
          val ldaModel = lda.run(corpus)
          val elapsed = (System.nanoTime() - startTime) / 1e6

          println(s"Finished training LDA model.  Summary:")
          val millis = (elapsed % 1000).toInt
          val seconds = ((elapsed / 1000) % 60).toInt
          val minutes = ((elapsed / (1000 * 60)) % 60).toInt
          val hours = ((elapsed / (1000 * 60 * 60)) % 24).toInt
          println(s"\t Training time: $hours hour(s) $minutes minute(s) $seconds second(s) and $millis milliseconds")

          if (ldaModel.isInstanceOf[LDAModel]) {
            val distLDAModel = ldaModel.asInstanceOf[LDAModel]
            val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
            println(s"\t Training data average log likelihood: $avgLogLikelihood")
            println()
          }

          // Print the topics, showing the top-weighted terms for each topic.
          val topicIndices = ldaModel.describeTopics(params.twords)
          val topics = topicIndices.map {
            case (terms, termWeights) =>
              terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
          }
          println(s"${params.K} topics:")
          topics.zipWithIndex.foreach {
            case (topic, i) =>
              println(s"TOPIC $i")
              topic.foreach {
                case (term, weight) =>
                  println(s"$term\t$weight")
              }
              println()
          }

          spark.stop()
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