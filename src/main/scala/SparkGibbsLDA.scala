package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import main.scala.obj.Dictionary
import main.scala.obj.Document
import main.scala.helper.LDACmdOption
import main.java.commons.cli.MissingOptionException
import main.java.commons.cli.MissingArgumentException
import main.java.commons.cli.CommandLine
import main.java.commons.cli.UnrecognizedOptionException
import main.scala.obj.Parameter

object SparkGibbsLDA {
  def main(args: Array[String]): Unit = {
    //println("Current directory: " + System.getProperty("user.dir"))
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
          println("ERROR!!! Phai nhap day du cac tham so: alpha, beta, directory, datafile, ntopics, niters")
          LDACmdOption.showHelp()
          return
        } else {

          //~~~~~~~~~~~ Timer ~~~~~~~~~~~
          val startTime = System.currentTimeMillis()

          //~~~~~~~~~~~ Body ~~~~~~~~~~~
          println("#################### DAY LA PHAN THAN CUA CHUONG TRINH ####################")

          if (cmd.hasOption("inference")) {
            // inf
            var inferencer = new Inferencer()
            inferencer.init(params)

            var newModel = inferencer.inference(params)

            for (i <- 0 until newModel.phi.length) {
              //phi: K * V
              println("-----------------------\ntopic" + i + " : ");
              for (j <- 0 until 10) {
                println(inferencer.globalDict.id2word.get(j).get + "\t" + newModel.phi(i)(j))
              }
            }
          } else { // est or estc
            // default: est
            var estimate = new Estimator
            estimate.init(cmd.hasOption("estcon"), params)
            estimate.estimate(params.savestep)
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
}