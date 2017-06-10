package main.scala.obj

import breeze.linalg.{ all, normalize, sum, DenseMatrix => BDM, DenseVector => BDV }
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import main.scala.helper.{ LDAOptimizer, Utils }
import scala.util.control.Breaks._

object LDA {
  /*
    BASED ON SPARK DEVELOPERS NOTE:

    This implementation uses GraphX, where the graph is bipartite with 2 types of vertices:
     - Document vertices
        - indexed with unique indices >= 0
        - Store vectors of length k (# topics).
     - Term vertices
        - indexed {-1, -2, ..., -vocabSize}
        - Store vectors of length k (# topics).
     - Edges correspond to terms appearing in documents.
        - Edges are directed Document -> Term.
        - Edges are partitioned by documents.

    Info on EM implementation.
     - We follow Section 2.2 from Asuncion et al., 2009.  We use some of their notation.
     - In this implementation, there is one edge for every unique term appearing in a document,
       i.e., for every unique (document, term) pair.
     - Notation:
        - N_{wkj} = count of tokens of term w currently assigned to topic k in document j
        - N_{*} where * is missing a subscript w/k/j is the count summed over missing subscript(s)
        - gamma_{wjk} = P(z_i = k | x_i = w, d_i = j),
          the probability of term x_i in document d_i having topic z_i.
     - Data graph
        - Document vertices store N_{kj}
        - Term vertices store N_{wk}
        - Edges store N_{wj}.
        - Global data N_k
     - Algorithm
        - Initial state:
           - Document and term vertices store random counts N_{wk}, N_{kj}.
        - E-step: For each (document,term) pair i, compute P(z_i | x_i, d_i).
           - Aggregate N_k from term vertices.
           - Compute gamma_{wjk} for each possible topic k, from each triplet.
             using inputs N_{wk}, N_{kj}, N_k.
        - M-step: Compute sufficient statistics for hidden parameters phi and theta
          (counts N_{wk}, N_{kj}, N_k).
           - Document update:
              - N_{kj} <- sum_w N_{wj} gamma_{wjk}
              - N_j <- sum_k N_{kj}  (only needed to output predictions)
           - Term update:
              - N_{wk} <- sum_j N_{wj} gamma_{wjk}
              - N_k <- sum_w N_{wk}

    TODO: Add simplex constraints to allow alpha in (0,1).
          See: Vorontsov and Potapenko. "Tutorial on Probabilistic Topic Modeling : Additive
               Regularization for Stochastic Matrix Factorization." 2014.
   */

  var ldaOptimizer = new LDAOptimizer

  /**
   * Vector over topics (length k) of token counts.
   * The meaning of these counts can vary, and it may or may not be normalized to be a distribution.
   */
  type TopicCounts = BDV[Double]

  type TokenCount = Double

  /** Term vertex IDs are {-1, -2, ..., -vocabSize} */
  def term2index(term: Int, distance: Long): Long = -(1 + term.toLong + distance)

  def index2term(termIndex: Long, distance: Long): Int = -(1 + termIndex + distance).toInt

  def isDocumentVertex(v: (VertexId, _)): Boolean = v._1 >= 0

  def isTermVertex(v: (VertexId, _)): Boolean = v._1 < 0

  /**
   * Compute gamma_{wjk}, a distribution over topics k.
   */
  def computePTopic(
    docTopicCounts: TopicCounts,
    termTopicCounts: TopicCounts,
    totalTopicCounts: TopicCounts,
    vocabSize: Int,
    eta: Double,
    alpha: Double): TopicCounts = {
    val K = docTopicCounts.length
    val N_j = docTopicCounts.data
    val N_w = termTopicCounts.data
    val N = totalTopicCounts.data
    val Weta = vocabSize * eta
    val deltaTopic = BDV.fill[Double](K)(0d)
    for (numTopic <- 0 until N_w.length) { // for each old topic
      for (numWord <- 0 until N_w(numTopic).toInt) { // for each word in this topic
        var gamma_wj = new Array[Double](K)
        //do multinominal sampling via cumulative method
        for (k <- 0 until K) {
          gamma_wj(k) = (N_w(k) - 1 + eta) * (N_j(k) - 1 + alpha) / (N(k) - 1 + Weta)
        }
        // cumulate multinomial parameters
        for (k <- 1 until K) {
          gamma_wj(k) += gamma_wj(k - 1)
        }
        // scaled sample because of unnormalized p[]
        val scale = Math.random() * gamma_wj(K - 1)
        //sample topic w.r.t distribution p
        var newTopic = 0
        if (gamma_wj(0) <= scale) {
          var low = 0
          var high = K - 1
          breakable {
            while (low <= high) {
              if (low == high - 1) {
                newTopic = high
                break
              }
              val mid = (low + high) / 2
              if (gamma_wj(mid) > scale) high = mid
              else low = mid
            }
          }
        }

        if (newTopic != numTopic) {
          // update change of topic via deltaTopic
          val delta = BDV.fill[Double](K)(0d)
          delta(numTopic) -= 1
          delta(newTopic) += 1
          deltaTopic += delta
        } else deltaTopic += BDV.fill[Double](K)(0d) // not change anything
      } // end for each word in this topic
    } // end for each old topic
    return deltaTopic
  }
}

class LDA private (
    private var k: Int,
    private var maxIterations: Int,
    private var docConcentration: Vector,
    private var topicConcentration: Double,
    private var seed: Long,
    private var checkpointInterval: Int,
    private var ldaOptimizer: LDAOptimizer) {

  /**
   * Constructs a LDA instance with default parameters.
   */
  def this() = this(k = 10, maxIterations = 20, docConcentration = Vectors.dense(-1),
    topicConcentration = -1, seed = Utils.nextLong(), checkpointInterval = 10,
    ldaOptimizer = new LDAOptimizer)

  /**
   * Number of topics to infer, i.e., the number of soft cluster centers.
   */
  def getK: Int = k

  /**
   * Set the number of topics to infer, i.e., the number of soft cluster centers.
   * (default = 10)
   */
  def setK(k: Int): this.type = {
    require(k > 0, s"LDA k (number of clusters) must be > 0, but was set to $k")
    this.k = k
    this
  }

  /**
   * Concentration parameter (commonly named "alpha") for the prior placed on documents'
   * distributions over topics ("theta").
   *
   * This is the parameter to a Dirichlet distribution.
   */
  def getAsymmetricDocConcentration: Vector = this.docConcentration

  /**
   * Concentration parameter (commonly named "alpha") for the prior placed on documents'
   * distributions over topics ("theta").
   *
   * This method assumes the Dirichlet distribution is symmetric and can be described by a single
   * [[Double]] parameter. It should fail if docConcentration is asymmetric.
   */
  def getDocConcentration: Double = {
    val parameter = docConcentration(0)
    if (docConcentration.size == 1) {
      parameter
    } else {
      require(docConcentration.toArray.forall(_ == parameter))
      parameter
    }
  }

  /**
   * Concentration parameter (commonly named "alpha") for the prior placed on documents'
   * distributions over topics ("theta").
   *
   * This is the parameter to a Dirichlet distribution, where larger values mean more smoothing
   * (more regularization).
   *
   * If set to a singleton vector Vector(-1), then docConcentration is set automatically. If set to
   * singleton vector Vector(t) where t != -1, then t is replicated to a vector of length k during
   * `LDAOptimizer.initialize()`. Otherwise, the [[docConcentration]] vector must be length k.
   * (default = Vector(-1) = automatic)
   *
   * Optimizer-specific parameter settings:
   *  - EM
   *     - Currently only supports symmetric distributions, so all values in the vector should be
   *       the same.
   *     - Values should be greater than 1.0
   *     - default = uniformly (50 / k) + 1, where 50/k is common in LDA libraries and +1 follows
   *       from Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *  - Online
   *     - Values should be greater than or equal to 0
   *     - default = uniformly (1.0 / k), following the implementation from
   *       <a href="https://github.com/Blei-Lab/onlineldavb">here</a>.
   */
  def setDocConcentration(docConcentration: Vector): this.type = {
    require(docConcentration.size == 1 || docConcentration.size == k,
      s"Size of docConcentration must be 1 or ${k} but got ${docConcentration.size}")
    this.docConcentration = docConcentration
    this
  }

  /**
   * Replicates a [[Double]] docConcentration to create a symmetric prior.
   */
  def setDocConcentration(docConcentration: Double): this.type = {
    this.docConcentration = Vectors.dense(docConcentration)
    this
  }

  /**
   * Alias for [[getAsymmetricDocConcentration]]
   */
  def getAsymmetricAlpha: Vector = getAsymmetricDocConcentration

  /**
   * Alias for [[getDocConcentration]]
   */
  def getAlpha: Double = getDocConcentration

  /**
   * Alias for `setDocConcentration()`
   */
  def setAlpha(alpha: Vector): this.type = setDocConcentration(alpha)

  /**
   * Alias for `setDocConcentration()`
   */
  def setAlpha(alpha: Double): this.type = setDocConcentration(alpha)

  /**
   * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
   * distributions over terms.
   *
   * This is the parameter to a symmetric Dirichlet distribution.
   *
   * @note The topics' distributions over terms are called "beta" in the original LDA paper
   * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
   */
  def getTopicConcentration: Double = this.topicConcentration

  /**
   * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
   * distributions over terms.
   *
   * This is the parameter to a symmetric Dirichlet distribution.
   *
   * @note The topics' distributions over terms are called "beta" in the original LDA paper
   * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
   *
   * If set to -1, then topicConcentration is set automatically.
   *  (default = -1 = automatic)
   *
   * Optimizer-specific parameter settings:
   *  - EM
   *     - Value should be greater than 1.0
   *     - default = 0.1 + 1, where 0.1 gives a small amount of smoothing and +1 follows
   *       Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *  - Online
   *     - Value should be greater than or equal to 0
   *     - default = (1.0 / k), following the implementation from
   *       <a href="https://github.com/Blei-Lab/onlineldavb">here</a>.
   */
  def setTopicConcentration(topicConcentration: Double): this.type = {
    this.topicConcentration = topicConcentration
    this
  }

  /**
   * Alias for [[getTopicConcentration]]
   */
  def getBeta: Double = getTopicConcentration

  /**
   * Alias for `setTopicConcentration()`
   */
  def setBeta(beta: Double): this.type = setTopicConcentration(beta)

  /**
   * Maximum number of iterations allowed.
   */
  def getMaxIterations: Int = maxIterations

  /**
   * Set the maximum number of iterations allowed.
   * (default = 20)
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations >= 0,
      s"Maximum of iterations must be nonnegative but got ${maxIterations}")
    this.maxIterations = maxIterations
    this
  }

  /**
   * Random seed for cluster initialization.
   */
  def getSeed: Long = seed

  /**
   * Set the random seed for cluster initialization.
   */
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Period (in iterations) between checkpoints.
   */
  def getCheckpointInterval: Int = checkpointInterval

  /**
   * Parameter for set checkpoint interval (greater than or equal to 1) or disable checkpoint (-1).
   * E.g. 10 means that the cache will get checkpointed every 10 iterations. Checkpointing helps
   * with recovery (when nodes fail). It also helps with eliminating temporary shuffle files on
   * disk, which can be important when LDA is run for many iterations. If the checkpoint directory
   * is not set in [[org.apache.spark.SparkContext]], this setting is ignored. (default = 10)
   *
   * @see [[org.apache.spark.SparkContext#setCheckpointDir]]
   */
  def setCheckpointInterval(checkpointInterval: Int): this.type = {
    require(checkpointInterval == -1 || checkpointInterval > 0,
      s"Period between checkpoints must be -1 or positive but got ${checkpointInterval}")
    this.checkpointInterval = checkpointInterval
    this
  }

  /**
   * Learn an LDA model using the given dataset.
   *
   * @param documents  RDD of documents, which are term (word) count vectors paired with IDs.
   *                   The term count vectors are "bags of words" with a fixed-size vocabulary
   *                   (where the vocabulary size is the length of the vector).
   *                   Document IDs must be unique and greater than or equal to 0.
   * @param maxIterations maximum iterations of LDA
   * @return  Inferred LDA model
   */
  def run(documents: RDD[(Long, Vector)], vocabSize: Long): LDAModel = {
    val state = ldaOptimizer.initialize(documents, vocabSize, this)
    var iter = 0
    val iterationTimes = Array.fill[Double](maxIterations)(0)
    while (iter < maxIterations) {
      val start = System.nanoTime()
      state.next()
      val elapsedSeconds = (System.nanoTime() - start) / 1e9
      iterationTimes(iter) = elapsedSeconds
      iter += 1
    }
    state.getLDAModel(iterationTimes)
  }
}