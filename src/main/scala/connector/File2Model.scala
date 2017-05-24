package main.scala.connector

import java.io.BufferedReader
import java.io.FileReader
import java.util.StringTokenizer
import java.io.File
import main.scala.obj.LDADataset
import scala.collection.mutable.ArrayBuffer
import main.scala.obj.Document
import main.scala.obj.Dictionary
import org.apache.spark.SparkContext

class File2Model {
  //---------------------------------------------------------------
  //	Parameters
  //---------------------------------------------------------------
  // link to a dataset
  var data: LDADataset = null
  //dataset size (i.e., number of docs)
  var M: Int = 0
  //vocabulary size
  var V: Int = 0
  //number of topics
  var K: Int = 0
  //LDA  hyperparameters
  var alpha: Double = 0.0d
  var beta: Double = 0.0d
  //the iteration at which the model was saved	
  var liter: Int = 0
  //topic assignments for words, size M x doc.size()
  var z: Array[Array[Int]] = null
  //---------------------------------------------------------------
  //	I/O Methods
  //---------------------------------------------------------------
  /**
   * read other file to get parameters
   */
  def readOthersFile(otherFile: String): Boolean = {
    //open file <model>.others to read:
    val reader = new BufferedReader(new FileReader(otherFile))
    var line: String = reader.readLine
    while (line != null) {
      var tknr = new StringTokenizer(line, "= \t\r\n");

      val count = tknr.countTokens
      if (count == 2) {

        val optstr = tknr.nextToken
        val optval = tknr.nextToken

        if (optstr.equalsIgnoreCase("alpha")) {
          alpha = optval.toDouble
        } else if (optstr.equalsIgnoreCase("beta")) {
          beta = optval.toDouble
        } else if (optstr.equalsIgnoreCase("ntopics")) {
          K = optval.toInt
        } else if (optstr.equalsIgnoreCase("liter")) {
          liter = optval.toInt
        } else if (optstr.equalsIgnoreCase("nwords")) {
          V = optval.toInt
        } else if (optstr.equalsIgnoreCase("ndocs")) {
          M = optval.toInt
        }
      }
      line = reader.readLine()
    }

    reader.close()
    true
  }

  def readTAssignFile(sc: SparkContext, tassignFile: String): Boolean = {
    var i, j: Int = 0
    val reader = new BufferedReader(new FileReader(tassignFile))

    var line: String = ""
    z = new Array[Array[Int]](M)
    data = new LDADataset(sc, M)
    data.V = V
    for (i <- 0 until M) {
      line = reader.readLine
      val tknr = new StringTokenizer(line, " \t\r\n")

      val length = tknr.countTokens

      var words = new ArrayBuffer[Int]
      var topics = new ArrayBuffer[Int]

      for (j <- 0 until length) {
        val token = tknr.nextToken();

        val tknr2 = new StringTokenizer(token, ":");
        if (tknr2.countTokens != 2) {
          System.out.println("Invalid word-topic assignment line\n");
          false
        }

        words.append(tknr2.nextToken.toInt)
        topics.append(tknr2.nextToken.toInt)
      } //end for each topic assignment

      //allocate and add new document to the corpus
      val doc = new Document(words)
      data.setDoc(doc, i)

      //assign values for z
      z(i) = topics.map(x => x).toArray

    } //end for each doc

    reader.close
    true
  }

  /**
   * load saved model
   */
  def loadModel(sc: SparkContext, dir: String, modelName: String, othersSuffix: String, tassignSuffix: String, wordMapFile: String): Boolean = {
    if (!readOthersFile(dir + File.separator + "output" + File.separator + modelName + othersSuffix))
      false

    if (!readTAssignFile(sc, dir + File.separator + "output" + File.separator + modelName + tassignSuffix))
      false

    // read dictionary
    //data.localDict = new Dictionary(sc, File2Dictionary.readWordMap(dir + File.separator + "output" + File.separator + wordMapFile))

    true
  }

  /**
   * get LDA Dataset
   */
  def getLDADataset(): LDADataset = data

  /**
   * Get M (number of docs) parameter
   */
  def getM(): Int = M

  /**
   * Get V (vocabulary size) parameter
   */
  def getV(): Int = V

  /**
   * Get K (number of topics) parameter
   */
  def getK(): Int = K

  /**
   * Get alpha (LDA hyperparameter) parameter
   */
  def getAlpha(): Double = alpha

  /**
   * Get beta (LDA hyperparameter) parameter
   */
  def getBeta(): Double = beta

  /**
   * Get liter (last iteration) parameter
   */
  def getLIter(): Int = liter

  /**
   * Get z (topic assignments for words) parameter
   */
  def getZ(): Array[Array[Int]] = z
}