package main.scala.connector

import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.spark.rdd.RDD

object Dictionary2File {
  //---------------------------------------------------
  // I/O methods
  //---------------------------------------------------
  /**
   * write dictionary to file
   */
  def writeWordMap(wordMapFile: String, word2idRDD: RDD[(String, Int)]): Boolean = {
    val word2id = word2idRDD.collect()
    val file = new File(wordMapFile)
    val writer = new BufferedWriter(new FileWriter(file))
    writer.flush
    //write number of words
    writer.write(word2id.size + "\n")

    //write word to id
    word2id.foreach(item => {
      writer.write(item._1 + " " + item._2 + "\n")
    })

    writer.close
    true
  }
}