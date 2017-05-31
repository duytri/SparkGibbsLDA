package main.scala.connector

import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.spark.rdd.RDD

object WordMap2File {
  //---------------------------------------------------
  // I/O methods
  //---------------------------------------------------
  /**
   * write dictionary to file
   */
  def writeWordMap(wordMapFile: String, word2id: Map[String, Int]): Unit = {
    println("WordMap File: " + wordMapFile)
    val file = new File(wordMapFile)
    file.getParentFile.mkdirs()
    file.createNewFile
    val writer = new BufferedWriter(new FileWriter(file))
    writer.flush
    //write number of words
    writer.write(word2id.size + "\n")

    //write word to id
    word2id.foreach(item => {
      writer.write(item._1 + " " + item._2 + "\n")
    })

    writer.close
  }
}