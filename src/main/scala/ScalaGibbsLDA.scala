package main.scala

import main.scala.obj.Dictionary
import main.scala.obj.Document
import main.scala.helper.LDACmdOption
import main.java.commons.cli.MissingOptionException
import main.java.commons.cli.MissingArgumentException
import main.java.commons.cli.CommandLine
import main.java.commons.cli.Option
import main.java.commons.cli.UnrecognizedOptionException
import main.scala.obj.Parameter
import main.scala.helper.Utils

object ScalaGibbsLDA {
  def main(args: Array[String]): Unit = {
    println("Current directory: " + System.getProperty("user.dir"))
    println("#################### Gibbs sampling LDA ####################")
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
          //println("#################### DAY LA PHAN THAN CUA CHUONG TRINH ####################")

          if (cmd.hasOption("inference")) {
            // inf
            var inferencer = new Inferencer()
            println("Preparing...")
            inferencer.init(params)
            println("Inferencing...")
            var newModel = inferencer.inference(params)

            for (i <- 0 until newModel.phi.length) {
              //phi: K * V
              println("-----------------------\nTopic " + (i + 1) + "th: ")
              Utils.printTopWords(i, newModel.V, newModel.phi, newModel.data, 10)
            }
          } else { // est or estc
            // default: est
            var estimate = new Estimator
            println("Preparing...")
            estimate.init(cmd.hasOption("estcon"), params)
            println("Estimating...")
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
