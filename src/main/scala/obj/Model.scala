package main.scala.obj

import org.apache.spark.mllib.linalg.{ Matrices, Matrix, Vector, Vectors }
import org.apache.spark.mllib.util.{ Loader, Saveable }
import org.apache.spark.graphx.{ Edge, EdgeContext, Graph, VertexId }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.hadoop.fs.Path

import breeze.linalg.{ argmax, argtopk, normalize, sum, DenseMatrix => BDM, DenseVector => BDV }
import breeze.numerics.{ exp, lgamma }

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import main.scala.helper.Utils
import main.scala.helper.BPQ

abstract class Model {

  /** Number of topics */
  def k: Int

  /** Vocabulary size (number of terms or terms in the vocabulary) */
  def vocabSize: Int

  /**
   * Concentration parameter (commonly named "alpha") for the prior placed on documents'
   * distributions over topics ("theta").
   *
   * This is the parameter to a Dirichlet distribution.
   */
  def docConcentration: Vector

  /**
   * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
   * distributions over terms.
   *
   * This is the parameter to a symmetric Dirichlet distribution.
   *
   * @note The topics' distributions over terms are called "beta" in the original LDA paper
   * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
   */
  def topicConcentration: Double

  /**
   * Shape parameter for random initialization of variational parameter gamma.
   * Used for variational inference for perplexity and other test-time computations.
   */
  //protected def gammaShape: Double

  /**
   * Inferred topics, where each topic is represented by a distribution over terms.
   * This is a matrix of size vocabSize x k, where each column is a topic.
   * No guarantees are given about the ordering of the topics.
   */
  def topicsMatrix: Matrix

  /**
   * Return the topics described by weighted terms.
   *
   * @param maxTermsPerTopic  Maximum number of terms to collect for each topic.
   * @param eta  Topics' distributions over terms.
   * @param termSize  Actual terms size.
   * @return  Array over topics.  Each topic is represented as a pair of matching arrays:
   *          (term indices, term weights in topic).
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  def describeTopics(maxTermsPerTopic: Int): Array[Array[(Int, Double)]]

  /**
   * Return the topics described by weighted terms.
   *
   * WARNING: If vocabSize and k are large, this can return a large object!
   *
   * @return  Array over topics.  Each topic is represented as a pair of matching arrays:
   *          (term indices, term weights in topic).
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  def describeTopics(): Array[Array[(Int, Double)]] = describeTopics(vocabSize)
}

/**
 * Distributed LDA model.
 * This model stores the inferred topics, the full training dataset, and the topic distributions.
 */
class LDAModel(
  val graph: Graph[LDA.TopicCounts, LDA.TokenCount],
  val globalTopicTotals: LDA.TopicCounts,
  val k: Int,
  val vocabSize: Int,
  override val docConcentration: Vector,
  override val topicConcentration: Double,
  val iterationTimes: Array[Double],
  val checkpointFiles: Array[String] = Array.empty[String])
    extends Model {

  import LDA._

  /**
   * Inferred topics, where each topic is represented by a distribution over terms.
   * This is a matrix of size vocabSize x k, where each column is a topic.
   * No guarantees are given about the ordering of the topics.
   *
   * WARNING: This matrix is collected from an RDD. Beware memory usage when vocabSize, k are large.
   */
  lazy val topicsMatrix: Matrix = {
    // Collect row-major topics
    val termTopicCounts: Array[(Int, TopicCounts)] =
      graph.vertices.filter(_._1 < 0).map {
        case (termIndex, cnts) =>
          (index2term(termIndex, vocabSize * getDocId(termIndex)), cnts)
      }.collect()
    // Convert to Matrix
    val brzTopics = BDM.zeros[Double](vocabSize, k)
    termTopicCounts.foreach {
      case (term, cnts) =>
        var j = 0
        while (j < k) {
          brzTopics(term, j) = cnts(j)
          j += 1
        }
    }
    Utils.matrixFromBreeze(brzTopics)
  }

  def getDocId(termId: Long): Long = {
    graph.edges.filter(_.dstId == termId).collect.apply(0).srcId
  }

  override def describeTopics(maxTermsPerTopic: Int): Array[Array[(Int, Double)]] = {
    val numTopics = k
    val phi = computePhi
    val result = Array.ofDim[(Int, Double)](k, maxTermsPerTopic)
    for (topic <- 0 until k) {
      val maxVertexPerTopic = phi.filter(_._1 == topic).takeOrdered(maxTermsPerTopic)(Ordering[Double].reverse.on(_._3))
      result(topic) = maxVertexPerTopic.map {
        case (topicId, termId, phi) =>
          (index2term(termId, vocabSize * getDocId(termId)), phi)
      }
    }
    return result
  }

  /**
   * Return the top documents for each topic
   *
   * @param maxDocumentsPerTopic  Maximum number of documents to collect for each topic.
   * @return  Array over topics.  Each element represent as a pair of matching arrays:
   *          (IDs for the documents, weights of the topic in these documents).
   *          For each topic, documents are sorted in order of decreasing topic weights.
   */
  def topDocumentsPerTopic(maxDocumentsPerTopic: Int): Array[(Array[Long], Array[Double])] = {
    val numTopics = k
    val topicsInQueues: Array[BPQ[(Double, Long)]] =
      topicDistributions.mapPartitions { docVertices =>
        // For this partition, collect the most common docs for each topic in queues:
        //  queues(topic) = queue of (doc topic, doc ID).
        val queues =
          Array.fill(numTopics)(new BPQ[(Double, Long)](maxDocumentsPerTopic))
        for ((docId, docTopics) <- docVertices) {
          var topic = 0
          while (topic < numTopics) {
            queues(topic) += (docTopics(topic) -> docId)
            topic += 1
          }
        }
        Iterator(queues)
      }.treeReduce { (q1, q2) =>
        q1.zip(q2).foreach { case (a, b) => a ++= b }
        q1
      }
    topicsInQueues.map { q =>
      val (docTopics, docs) = q.toArray.sortBy(-_._1).unzip
      (docs.toArray, docTopics.toArray)
    }
  }

  /**
   * Return the top topic for each (doc, term) pair.  I.e., for each document, what is the most
   * likely topic generating each term?
   *
   * @return RDD of (doc ID, assignment of top topic index for each term),
   *         where the assignment is specified via a pair of zippable arrays
   *         (term indices, topic indices).  Note that terms will be omitted if not present in
   *         the document.
   */
  lazy val topicAssignments: RDD[(Long, Array[Int], Array[Int])] = {
    // For reference, compare the below code with the core part of EMLDAOptimizer.next().
    val eta = topicConcentration
    val W = vocabSize
    val alpha = docConcentration(0)
    val N_k = globalTopicTotals
    val sendMsg: EdgeContext[TopicCounts, TokenCount, (Array[Int], Array[Int])] => Unit =
      (edgeContext) => {
        // E-STEP: Compute gamma_{wjk} (smoothed topic distributions).
        val scaledTopicDistribution: TopicCounts =
          computePTopic(edgeContext.srcAttr, edgeContext.dstAttr, N_k, W, eta, alpha)
        // For this (doc j, term w), send top topic k to doc vertex.
        val topTopic: Int = argmax(scaledTopicDistribution)
        val term: Int = index2term(edgeContext.dstId, edgeContext.srcId * W)
        edgeContext.sendToSrc((Array(term), Array(topTopic)))
      }
    val mergeMsg: ((Array[Int], Array[Int]), (Array[Int], Array[Int])) => (Array[Int], Array[Int]) =
      (terms_topics0, terms_topics1) => {
        (terms_topics0._1 ++ terms_topics1._1, terms_topics0._2 ++ terms_topics1._2)
      }
    // M-STEP: Aggregation computes new N_{kj}, N_{wk} counts.
    val perDocAssignments =
      graph.aggregateMessages[(Array[Int], Array[Int])](sendMsg, mergeMsg).filter(isDocumentVertex)
    perDocAssignments.map {
      case (docID: Long, (terms: Array[Int], topics: Array[Int])) =>
        // TODO: Avoid zip, which is inefficient.
        val (sortedTerms, sortedTopics) = terms.zip(topics).sortBy(_._1).unzip
        (docID, sortedTerms.toArray, sortedTopics.toArray)
    }
  }

  // TODO
  // override def logLikelihood(documents: RDD[(Long, Vector)]): Double = ???

  /**
   * Log likelihood of the observed tokens in the training set,
   * given the current parameter estimates:
   *  log P(docs | topics, topic distributions for docs, alpha, eta)
   *
   * Note:
   *  - This excludes the prior; for that, use [[logPrior]].
   *  - Even with [[logPrior]], this is NOT the same as the data log likelihood given the
   *    hyperparameters.
   */
  lazy val logLikelihood: Double = {
    // TODO: generalize this for asymmetric (non-scalar) alpha
    val alpha = this.docConcentration(0) // To avoid closure capture of enclosing object
    val eta = this.topicConcentration
    val N_k = globalTopicTotals
    val smoothed_N_k: TopicCounts = N_k + (vocabSize * eta)
    // Edges: Compute token log probability from phi_{wk}, theta_{kj}.
    val sendMsg: EdgeContext[TopicCounts, TokenCount, Double] => Unit = (edgeContext) => {
      val N_wj = edgeContext.attr
      val smoothed_N_wk: TopicCounts = edgeContext.dstAttr + eta - 1.0
      val smoothed_N_kj: TopicCounts = edgeContext.srcAttr + alpha - 1.0
      val phi_wk: TopicCounts = smoothed_N_wk :/ smoothed_N_k
      val theta_kj: TopicCounts = normalize(smoothed_N_kj, 1.0)
      val tokenLogLikelihood = N_wj * math.log(phi_wk.dot(theta_kj))
      edgeContext.sendToDst(tokenLogLikelihood)
    }
    graph.aggregateMessages[Double](sendMsg, _ + _)
      .map(_._2).fold(0.0)(_ + _)
  }

  /**
   * Log probability of the current parameter estimate:
   * log P(topics, topic distributions for docs | alpha, eta)
   */
  lazy val logPrior: Double = {
    // TODO: generalize this for asymmetric (non-scalar) alpha
    val alpha = this.docConcentration(0) // To avoid closure capture of enclosing object
    val eta = this.topicConcentration
    // Term vertices: Compute phi_{wk}.  Use to compute prior log probability.
    // Doc vertex: Compute theta_{kj}.  Use to compute prior log probability.
    val N_k = globalTopicTotals
    val smoothed_N_k: TopicCounts = N_k + (vocabSize * (eta - 1.0))
    val seqOp: (Double, (VertexId, TopicCounts)) => Double = {
      case (sumPrior: Double, vertex: (VertexId, TopicCounts)) =>
        if (isTermVertex(vertex)) {
          val N_wk = vertex._2
          val smoothed_N_wk: TopicCounts = N_wk + (eta - 1.0)
          val phi_wk: TopicCounts = smoothed_N_wk :/ smoothed_N_k
          sumPrior + (eta - 1.0) * sum(phi_wk.map(math.log))
        } else {
          val N_kj = vertex._2
          val smoothed_N_kj: TopicCounts = N_kj + (alpha - 1.0)
          val theta_kj: TopicCounts = normalize(smoothed_N_kj, 1.0)
          sumPrior + (alpha - 1.0) * sum(theta_kj.map(math.log))
        }
    }
    graph.vertices.aggregate(0.0)(seqOp, _ + _)
  }

  /**
   * For each document in the training set, return the distribution over topics for that document
   * ("theta_doc").
   *
   * @return  RDD of (document ID, topic distribution) pairs
   */
  def topicDistributions: RDD[(Long, Vector)] = {
    graph.vertices.filter(LDA.isDocumentVertex).map {
      case (docID, topicCounts) =>
        (docID.toLong, Utils.vectorFromBreeze(topicCounts))
    }
  }

  /**
   * For each document, return the top k weighted topics for that document and their weights.
   * @return RDD of (doc ID, topic indices, topic weights)
   */
  def topTopicsPerDocument(k: Int): RDD[(Long, Array[Int], Array[Double])] = {
    graph.vertices.filter(LDA.isDocumentVertex).map {
      case (docID, topicCounts) =>
        // TODO: Remove work-around for the breeze bug.
        // https://github.com/scalanlp/breeze/issues/561
        val topIndices = if (k == topicCounts.length) {
          Seq.range(0, k)
        } else {
          argtopk(topicCounts, k)
        }
        val sumCounts = sum(topicCounts)
        val weights = if (sumCounts != 0) {
          topicCounts(topIndices) / sumCounts
        } else {
          topicCounts(topIndices)
        }
        (docID.toLong, topIndices.toArray, weights.toArray)
    }
  }

  def computeTheta(): RDD[(VertexId, Int, Double)] = {
    val alpha = this.docConcentration(0)
    graph.vertices.filter(LDA.isDocumentVertex).flatMap {
      case (docId, topicCounts) =>
        topicCounts.mapPairs {
          case (topicId, wordCounts) =>
            val thetaMK = ((wordCounts + alpha) / (topicCounts.data.sum + topicCounts.length * alpha))
            (docId, topicId, thetaMK)
        }.toArray
    }
  }

  def computePhi(): RDD[(Int, VertexId, Double)] = {
    val eta = this.topicConcentration
    val wordTopicCounts = this.globalTopicTotals
    val vocabSize = this.vocabSize
    graph.vertices.filter(LDA.isTermVertex).flatMap {
      case (termId, topicCounts) =>
        topicCounts.mapPairs {
          case (topicId, wordCounts) =>
            val phiKW = ((wordCounts + eta) / (wordTopicCounts.data(topicId) + vocabSize * eta))
            (topicId, termId, phiKW)
        }.toArray
    }
  }

  def computePerplexity(tokenSize: Long): Double = {
    val alpha = this.docConcentration(0) // To avoid closure capture of enclosing object
    val eta = this.topicConcentration
    val N_k = globalTopicTotals
    val smoothed_N_k: TopicCounts = N_k + (vocabSize * eta)
    // Edges: Compute token log probability from phi_{wk}, theta_{kj}.
    val sendMsg: EdgeContext[TopicCounts, TokenCount, Double] => Unit = (edgeContext) => {
      val N_wj = edgeContext.attr
      val smoothed_N_wk: TopicCounts = edgeContext.dstAttr + eta - 1.0
      val smoothed_N_kj: TopicCounts = edgeContext.srcAttr + alpha - 1.0
      val phi_wk: TopicCounts = smoothed_N_wk :/ smoothed_N_k
      val theta_kj: TopicCounts = normalize(smoothed_N_kj, 1.0)
      val tokenLogLikelihood = N_wj * math.log(phi_wk.dot(theta_kj))
      edgeContext.sendToDst(tokenLogLikelihood)
    }
    val docSum = graph.aggregateMessages[Double](sendMsg, _ + _)
      .map(_._2).fold(0.0)(_ + _)
    return math.exp(-1 * docSum / tokenSize)
  }

  def countGraphInfo() = {
    println("Number of document vertices: " + graph.vertices.filter(LDA.isDocumentVertex).count())
    println("Number of term vertices: " + graph.vertices.filter(LDA.isTermVertex).count())
    println("Number of edges: " + graph.edges.count())
  }
}