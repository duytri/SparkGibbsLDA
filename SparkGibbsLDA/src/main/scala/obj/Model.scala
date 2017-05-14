package main.scala.obj

import java.io.File
import main.scala.connector.File2LDADataset
import main.scala.helper.LDACmdOption
import scala.collection.mutable.ArrayBuffer
import main.scala.helper.Constants

/**
 * Lop bieu dien MODEL cua LDA
 * @param tassignSuffix suffix for topic assignment file
 * @param thetaSuffix suffix for theta (topic - document distribution) file
 * @param phiSuffix suffix for phi file (topic - word distribution) file
 * @param othersSuffix suffix for containing other parameters
 * @param twordsSuffix suffix for file containing words-per-topics (top words per topic)
 * @param wordMapFile file that contain word to id map
 * @param trainlogFile training log file
 * @param dir based directory
 * @param dfile training data file
 * @param modelName model name
 * @param modelStatus see Constants class for status of model
 * @param data link to a dataset
 * @param M dataset size (i.e., number of docs)
 * @param V vocabulary size
 * @param K number of topics
 * @param alpha, beta LDA  hyperparameters
 * @param niters number of Gibbs sampling iteration
 * @param liter the iteration at which the model was saved
 * @param savestep saving period
 * @param twords print out top words per each topic
 * @param theta document - topic distributions, size M x K
 * @param phi topic-word distributions, size K x V
 * @param z topic assignments for words, size M x doc.size()
 * @param nw nw[i][j]: number of instances of word/term i assigned to topic j, size V x K
 * @param nd nd[i][j]: number of words in document i assigned to topic j, size M x K
 * @param nwsum nwsum[j]: total number of words assigned to topic j, size K
 * @param ndsum ndsum[i]: total number of words in document i, size M
 */
class Model(var tassignSuffix: String, var thetaSuffix: String, var phiSuffix: String, var othersSuffix: String, var twordsSuffix: String, var wordMapFile: String, var trainlogFile: String, var dir: String, var dfile: String, var modelName: String, var modelStatus: Int, var data: LDADataset, var M: Int, var V: Int, var K: Int, var alpha: Double, var beta: Double, var niters: Int, var liter: Int, var savestep: Int, var twords: Int, var theta: Array[Array[Double]], var phi: Array[Array[Double]], var z: Array[Array[Int]], var nw: Array[Array[Int]], var nd: Array[Array[Int]], var nwsum: Array[Int], var ndsum: Array[Int], var p: Array[Double]) {

  /**
   * Set default values for variables
   */
  def this() = {
    this(".tassign", ".theta", ".phi", ".others", ".twords", "wordmap.txt", "trainlog.txt", "./", "trndocs.dat", "model-final", Constants.MODEL_STATUS_UNKNOWN, null, 0, 0, 100, 50.0 / 100, 0.1, 2000, 0, 100, 10, null, null, null, null, null, null, null, null)
  }
  /**
   * initialize the model
   */
  protected def init(): Boolean = {
    if (LDACmdOption.alpha == -1) false
    modelName = LDACmdOption.modelName
    K = LDACmdOption.K
    alpha = LDACmdOption.alpha
    if (alpha < 0.0) alpha = 50.0 / K
    if (LDACmdOption.beta >= 0) beta = LDACmdOption.beta
    niters = LDACmdOption.niters
    dir = LDACmdOption.dir
    if (dir.endsWith(File.separator)) dir = dir.substring(0, dir.length - 1)
    dfile = LDACmdOption.dfile
    twords = LDACmdOption.twords
    wordMapFile = LDACmdOption.wordMapFileName
    true
  }
}