package main.scala.connector

import scala.collection.mutable.ArrayBuffer
import main.scala.obj.LDADataset
import main.scala.obj.Dictionary
import java.io.File
import java.io.BufferedReader
import java.io.FileReader

object File2LDADataset {
  //---------------------------------------------------------------
  // I/O methods
  //---------------------------------------------------------------

  /**
   *  read a dataset from a stream, create new dictionary
   *  @return dataset if success and null otherwise
   */
  def readDataSet(filename: String): LDADataset = {
    var data = new LDADataset
    val file = new File(filename)
    val reader = new BufferedReader(new FileReader(file))

    var line = reader.readLine()
    val M = line.toInt

    data.M = M
    for (i <- 0 until M) {
      data.setDoc(reader.readLine(), i)
    }

    reader.close()
    data
  }

  /**
   * read a dataset from a file with a preknown vocabulary
   * @param filename file from which we read dataset
   * @param dict the dictionary
   * @return dataset if success and null otherwise
   */
  def readDataSet(filename: String, dict: Dictionary): LDADataset = {
    var data = readDataSet(filename)
    data.globalDict = dict
    data
  }

  /**
   *  read a dataset from a stream, create new dictionary
   *  @return dataset if success and null otherwise
   */
  def readDataSet(reader: BufferedReader): LDADataset = {
    var data = new LDADataset
    //read number of document
    var line = reader.readLine()
    val M = line.toInt

    data.M = M
    for (i <- 0 until M) {
      data.setDoc(reader.readLine(), i)
    }
    reader.close()
    data
  }

  /**
   * read a dataset from a stream with respect to a specified dictionary
   * @param reader stream from which we read dataset
   * @param dict the dictionary
   * @return dataset if success and null otherwise
   */
  def readDataSet(reader: BufferedReader, dict: Dictionary): LDADataset = {
    var data = readDataSet(reader)
    data.globalDict = dict
    data
  }

  /**
   * read a dataset from a string, create new dictionary
   * @param str String from which we get the dataset, documents are seperated by newline character
   * @return dataset if success and null otherwise
   */
  def readDataSet(strs: ArrayBuffer[String]): LDADataset = {
    var data = new LDADataset
    for (i <- 0 until strs.length) {
      data.setDoc(strs(i), i)
    }
    data
  }

  /**
   * read a dataset from a string with respect to a specified dictionary
   * @param str String from which we get the dataset, documents are seperated by newline character
   * @param dict the dictionary
   * @return dataset if success and null otherwise
   */
  def readDataSet(strs: ArrayBuffer[String], dict: Dictionary): LDADataset = {
    var data = readDataSet(strs)
    data.globalDict = dict
    data
  }
}