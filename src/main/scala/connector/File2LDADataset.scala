package main.scala.connector

import scala.collection.mutable.ArrayBuffer
import main.scala.obj.LDADataset
import main.scala.obj.Dictionary
import java.io.File
import java.io.BufferedReader
import java.io.FileReader
import org.apache.spark.SparkContext

object File2LDADataset {
  //---------------------------------------------------------------
  // I/O methods
  //---------------------------------------------------------------

  /**
   *  read a dataset from a stream, create new dictionary
   *  @return dataset if success and null otherwise
   */
  def readDataSet(sc: SparkContext, filename: String): LDADataset = {
    val file = new File(filename)
    val reader = new BufferedReader(new FileReader(file))

    var line = reader.readLine()
    val M = line.toInt
    var data = new LDADataset(sc, M)

    for (i <- 0 until M) {
      //data.setDoc(reader.readLine(), i)
    }

    reader.close()
    data
  }
}