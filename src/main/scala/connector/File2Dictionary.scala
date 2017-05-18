package main.scala.connector

import java.io.File
import java.io.FileReader
import java.io.BufferedReader
import java.util.StringTokenizer
import scala.collection.mutable.Map

object File2Dictionary {
  //---------------------------------------------------
  // I/O methods
  //---------------------------------------------------
  /**
   * read dictionary from file
   */
  def readWordMap(wordMapFile: String): Map[String, Int] = {
    var result: Map[String, Int] = null
    val file = new File(wordMapFile)
    val reader = new BufferedReader(new FileReader(file))

    //read the number of words
    var line = reader.readLine()
    val nwords = line.toInt

    //read map
    for(i<-0 until nwords){
      line = reader.readLine();
      val tknr = new StringTokenizer(line, " \t\n\r")

      if (tknr.countTokens() == 2)
        result.put(tknr.nextToken(), tknr.nextToken().toInt)
    }

    reader.close()
    result
  }
}