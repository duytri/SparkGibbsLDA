package main.scala.connector

import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.collection.mutable.Map

object Dictionary2File {
  //---------------------------------------------------
  // I/O methods
  //---------------------------------------------------
  /**
   * write dictionary to file
   */
  def writeWordMap(wordMapFile: String, word2id: Map[String, Int]): Boolean = {
    val file = new File(wordMapFile)
    val writer = new BufferedWriter(new FileWriter(file))
    writer.flush()
    //write number of words
    writer.write(word2id.size + "\n")

    //write word to id
    word2id.foreach(item => {
      writer.write(item._2 + " " + item._1 + "\n")
    })

    writer.close();
    true
  }
}