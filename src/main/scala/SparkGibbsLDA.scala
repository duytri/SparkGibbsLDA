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

object SparkGibbsLDA {
  def main(args: Array[String]): Unit = {
    println("#################### Collapsed Gibbs sampling LDA in Apache Spark ####################")
    try {
      val cmd = LDACmdOption.getArguments(args)
      if (cmd.hasOption("help")) {
        LDACmdOption.showHelp()
      } else {
        // set user parameters

        //~~~~~~~~~~~ Timer ~~~~~~~~~~~
        val startTime = System.currentTimeMillis()

        //~~~~~~~~~~~ Spark ~~~~~~~~~~~
        val conf = new SparkConf().setAppName("SparkGibbsLDA").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val data = Array(1, 2, 3, 4, 5)
        val distData = sc.parallelize(data)
        val doc = new Document
        println("#################### Tong cua mang la: " + distData.reduce(_ + _) + " ####################")

        //~~~~~~~~~~~ Timer ~~~~~~~~~~~
        val duration = System.currentTimeMillis() - startTime
        val millis = (duration % 1000).toInt
        val seconds = ((duration / 1000) % 60).toInt
        val minutes = ((duration / (1000 * 60)) % 60).toInt
        val hours = ((duration / (1000 * 60 * 60)) % 24).toInt
        println("#################### Finished in " + hours + " hour(s) " + minutes + " minute(s) " + seconds + " second(s) and " + millis + " millisecond(s) ####################")
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