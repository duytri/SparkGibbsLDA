package main.scala.connector

import java.io.BufferedReader
import java.io.FileReader
import java.util.StringTokenizer
import java.io.File
import main.scala.obj.LDADataset
import scala.collection.mutable.ArrayBuffer
import main.scala.obj.Document
import main.scala.obj.Dictionary

object File2Model {
  //---------------------------------------------------------------
  //	Parameters
  //---------------------------------------------------------------
  // link to a dataset
  var data: LDADataset = _
  //dataset size (i.e., number of docs)
  var M: Int = _
  //vocabulary size
  var V: Int = _
  //number of topics
  var K: Int = _
  //LDA  hyperparameters
  var alpha: Double = _
  var beta: Double = _
  //the iteration at which the model was saved	
  var liter: Int = _
  //topic assignments for words, size M x doc.size()
  var z: Array[Array[Int]] = _
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

  def readTAssignFile(tassignFile: String): Boolean = {
    var i, j: Int = 0
    val reader = new BufferedReader(new FileReader(tassignFile))

    var line: String = ""
    z = new Array[Array[Int]](M)
    data = new LDADataset(M)
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
	def loadModel(dir:String, modelName:String,othersSuffix:String,tassignSuffix:String,wordMapFile:String):Boolean={
		if (!readOthersFile(dir + File.separator + modelName + othersSuffix))
			false
		
		if (!readTAssignFile(dir + File.separator + modelName + tassignSuffix))
			false
		
		// read dictionary
		data.localDict = new Dictionary(File2Dictionary.readWordMap(dir + File.separator + wordMapFile))
		
		true
	}
}