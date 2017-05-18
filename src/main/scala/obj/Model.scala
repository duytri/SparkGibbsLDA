package main.scala.obj

import java.io.File
import main.scala.connector.File2LDADataset
import main.scala.helper.LDACmdOption
import scala.collection.mutable.ArrayBuffer
import main.scala.helper.Constants
import main.java.commons.cli.CommandLine
import main.scala.connector.File2LDADataset
import main.scala.connector.File2Model
import scala.util.Random

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

  //---------------------------------------------------------------
  //	Init Methods
  //---------------------------------------------------------------
  /**
   * initialize the model
   */
  def init(params: Parameter): Boolean = {
    if (params == null)
      return false

    modelName = params.modelname
    K = params.K

    alpha = params.alpha
    if (alpha < 0.0)
      alpha = 50.0 / K;

    if (params.beta >= 0)
      beta = params.beta

    niters = params.niters

    dir = params.directory
    if (dir.endsWith(File.separator))
      dir = dir.substring(0, dir.length - 1)

    dfile = params.datafile
    twords = params.twords
    wordMapFile = params.wordMapFileName

    return true
  }

  /**
   * Init parameters for estimation
   */
  def initNewModel(params: Parameter): Boolean = {
    if (!init(params))
      return false

    val random = new Random
    p = new Array[Double](K)

    data = File2LDADataset.readDataSet(dir + File.separator + dfile)
    if (data == null) {
      println("Fail to read training data!\n")
      return false
    }

    //+ allocate memory and assign values for variables		
    M = data.M
    V = data.V
    dir = params.directory
    savestep = params.savestep

    // K: from command line or default value
    // alpha, beta: from command line or default values
    // niters, savestep: from command line or default values

    nw = Array.ofDim[Int](V, K)
    for (w <- 0 until V) {
      for (k <- 0 until K) {
        nw(w)(k) = 0
      }
    }

    nd = Array.ofDim[Int](M, K)
    for (m <- 0 until M) {
      for (k <- 0 until K) {
        nd(m)(k) = 0
      }
    }

    nwsum = Array.ofDim[Int](K)
    for (k <- 0 until K) {
      nwsum(k) = 0
    }

    ndsum = Array.ofDim[Int](M)
    for (m <- 0 until M) {
      ndsum(m) = 0
    }

    z = Array.ofDim[Array[Int]](M)
    for (m <- 0 until M) {
      val N = data.docs(m).length
      //z(m) = new Array[Int]

      //initilize for z
      z(m) = Array.ofDim[Int](N)
      for (n <- 0 until N) {
        val topic = random.nextInt(K)
        z(m)(n) = topic

        // number of instances of word assigned to topic j
        nw(data.docs(m).words(n))(topic) += 1
        // number of words in document i assigned to topic j
        nd(m)(topic) += 1
        // total number of words assigned to topic j
        nwsum(topic) += 1
      }
      // total number of words in document i
      ndsum(m) = N
    }

    theta = Array.ofDim[Double](M, K)
    phi = Array.ofDim[Double](K, V)

    return true
  }

  /**
   * Init parameters for inference
   * @param newData DataSet for which we do inference
   */
  def initNewModel(params: Parameter, newData: LDADataset, trnModel: Model): Boolean = {
    if (!init(params))
      return false

    val random = new Random
    K = trnModel.K
    alpha = trnModel.alpha
    beta = trnModel.beta;

    p = new Array[Double](K)
    //println("K:" + K);

    data = newData

    //+ allocate memory and assign values for variables		
    M = data.M
    V = data.V
    dir = params.directory
    savestep = params.savestep
    //println("M:" + M);
    //println("V:" + V);

    // K: from command line or default value
    // alpha, beta: from command line or default values
    // niters, savestep: from command line or default values

    nw = Array.ofDim[Int](V, K)
    for (w <- 0 until V) {
      for (k <- 0 until K) {
        nw(w)(k) = 0
      }
    }

    nd = Array.ofDim[Int](M, K)
    for (m <- 0 until M) {
      for (k <- 0 until K) {
        nd(m)(k) = 0
      }
    }

    nwsum = Array.ofDim[Int](K)
    for (k <- 0 until K) {
      nwsum(k) = 0
    }

    ndsum = Array.ofDim[Int](M)
    for (m <- 0 until M) {
      ndsum(m) = 0
    }

    z = new Array[Array[Int]](M)
    for (m <- 0 until M) {
      val N = data.docs(m).length
      //z(m) = new Array[Int]

      //initilize for z
      z(m) = new Array[Int](N)
      for (n <- 0 until N) {
        val topic = random.nextInt(K)
        z(m)(n) = topic

        // number of instances of word assigned to topic j
        nw(data.docs(m).words(n))(topic) += 1
        // number of words in document i assigned to topic j
        nd(m)(topic) += 1
        // total number of words assigned to topic j
        nwsum(topic) += 1
      }
      // total number of words in document i
      ndsum(m) = N
    }

    theta = Array.ofDim[Double](M, K)
    phi = Array.ofDim[Double](K, V)

    return true
  }

  /**
   * Init parameters for inference
   * reading new dataset from file
   */
  def initNewModel(params: Parameter, trnModel: Model): Boolean = {
    if (!init(params))
      return false

    val dataset = File2LDADataset.readDataSet(dir + File.separator + dfile, trnModel.data.localDict)
    if (dataset == null) {
      println("Fail to read dataset!\n")
      return false
    }

    initNewModel(params, dataset, trnModel)
  }

  /**
   * init parameter for continue estimating or for later inference
   */
  def initEstimatedModel(params: Parameter): Boolean = {
    if (!init(params))
      return false

    p = new Array[Double](K)

    dir = params.directory
    modelName = params.modelname
    wordMapFile = params.wordMapFileName
    savestep = params.savestep
    // load model, i.e., read z and trndata
    if (!File2Model.loadModel(dir, modelName, othersSuffix, tassignSuffix, wordMapFile)) {
      System.out.println("Fail to load word-topic assignment file of the model!\n");
      return false
    }

    System.out.println("Model loaded:");
    System.out.println("\talpha:" + alpha);
    System.out.println("\tbeta:" + beta);
    System.out.println("\tM:" + M);
    System.out.println("\tV:" + V);

    nw = Array.ofDim[Int](V, K)
    for (w <- 0 until V) {
      for (k <- 0 until K) {
        nw(w)(k) = 0
      }
    }

    nd = Array.ofDim[Int](M, K)
    for (m <- 0 until M) {
      for (k <- 0 until K) {
        nd(m)(k) = 0
      }
    }

    nwsum = Array.ofDim[Int](K)
    for (k <- 0 until K) {
      nwsum(k) = 0
    }

    ndsum = Array.ofDim[Int](M)
    for (m <- 0 until M) {
      ndsum(m) = 0
    }

    for (m <- 0 until data.M) {
      val N = data.docs(m).length

      // assign values for nw, nd, nwsum, and ndsum
      for (n <- 0 until N) {
        val w = data.docs(m).words(n)
        val topic = z(m)(n)

        // number of instances of word i assigned to topic j
        nw(w)(topic) += 1
        // number of words in document i assigned to topic j
        nd(m)(topic) += 1
        // total number of words assigned to topic j
        nwsum(topic) += 1
      }
      // total number of words in document i
      ndsum(m) = N;
    }

    theta = Array.ofDim[Double](M, K)
    phi = Array.ofDim[Double](K, V)

    return true
  }
}