package main.scala.helper

import breeze.linalg.{ max, sum, DenseMatrix => BDM, DenseVector => BDV, CSCMatrix => BSM, Matrix => BM, SparseVector => BSV, Vector => BV }
import breeze.numerics._
import scala.util.Random
import org.apache.spark.mllib.linalg.{ DenseMatrix, SparseMatrix, DenseVector, SparseVector }
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{ CountVectorizer, CountVectorizerModel, RegexTokenizer }
import org.apache.spark.ml.linalg.{ Vector => MLVector }
import org.apache.spark.mllib.linalg.{ Matrix, Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.SparkContext

object Utils {
  /**
   * Log Sum Exp with overflow protection using the identity:
   * For any a: $\log \sum_{n=1}^N \exp\{x_n\} = a + \log \sum_{n=1}^N \exp\{x_n - a\}$
   */
  def logSumExp(x: BDV[Double]): Double = {
    val a = max(x)
    a + log(sum(exp(x :- a)))
  }

  /**
   * For theta ~ Dir(alpha), computes E[log(theta)] given alpha. Currently the implementation
   * uses [[breeze.numerics.digamma]] which is accurate but expensive.
   */
  def dirichletExpectation(alpha: BDV[Double]): BDV[Double] = {
    digamma(alpha) - digamma(sum(alpha))
  }

  /**
   * Computes [[dirichletExpectation()]] row-wise, assuming each row of alpha are
   * Dirichlet parameters.
   */
  def dirichletExpectation(alpha: BDM[Double]): BDM[Double] = {
    val rowSum = sum(alpha(breeze.linalg.*, ::))
    val digAlpha = digamma(alpha)
    val digRowSum = digamma(rowSum)
    val result = digAlpha(::, breeze.linalg.*) - digRowSum
    result
  }

  /**
   * Creates a Matrix instance from a breeze matrix.
   * @param breeze a breeze matrix
   * @return a Matrix instance
   */
  def matrixFromBreeze(breeze: BM[Double]): Matrix = {
    breeze match {
      case dm: BDM[Double] =>
        new DenseMatrix(dm.rows, dm.cols, dm.data, dm.isTranspose)
      case sm: BSM[Double] =>
        // There is no isTranspose flag for sparse matrices in Breeze
        new SparseMatrix(sm.rows, sm.cols, sm.colPtrs, sm.rowIndices, sm.data)
      case _ =>
        throw new UnsupportedOperationException(
          s"Do not support conversion from type ${breeze.getClass.getName}.")
    }
  }

  /**
   * Creates a vector instance from a breeze vector.
   */
  def vectorFromBreeze(breezeVector: BV[Double]): Vector = {
    breezeVector match {
      case v: BDV[Double] =>
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new DenseVector(v.data)
        } else {
          new DenseVector(v.toArray) // Can't use underlying array directly, so make a new one
        }
      case v: BSV[Double] =>
        if (v.index.length == v.used) {
          new SparseVector(v.length, v.index, v.data)
        } else {
          new SparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: BV[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }

  def asBreeze(values: Vector): BV[Double] = new BDV[Double](values.toArray)

  def nextLong(): Long = {
    val random = new Random
    random.nextLong()
  }

  /**
   * Random topic for same words in a document
   * @param K number of topic
   * @param wordCount number of the same word in a document
   * 
   * @return vector number of word randomly assigned to each topic 
   */
  def randomVectorInt(K: Int, wordCount: Int): BDV[Double] = {
    val gamma = BDV.fill[Double](K)(0d)
    for (w <- 0 until wordCount) {
      gamma(Math.floor(Math.random() * K).toInt) += 1d
    }
    gamma
  }

  /**
   * Load documents, tokenize them, create vocabulary, and prepare documents as term count vectors.
   * @return (corpus, vocabulary as array, total token count in corpus)
   */
  def preprocess(
    sc: SparkContext,
    paths: String): (RDD[(Long, Vector)], Array[String], Long) = {

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.implicits._

    // Get dataset of document texts
    // One document in each text file. If the input consists of many small files,
    // this can result in a large number of small partitions, which can degrade performance.
    // In this case, consider using coalesce() to create fewer, larger partitions.
    val df = sc.wholeTextFiles(paths).map(_._2.trim).toDF("docs")

    val tokenizer = new RegexTokenizer()
      .setInputCol("docs")
      .setOutputCol("tokens")

    val countVectorizer = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("features")
    //.setVocabSize(vocabSize)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, countVectorizer))

    val model = pipeline.fit(df)
    val documents = model.transform(df)
      .select("features")
      .rdd
      .map { case Row(features: MLVector) => Vectors.fromML(features) }
      .zipWithIndex()
      .map(_.swap)

    (documents,
      model.stages(1).asInstanceOf[CountVectorizerModel].vocabulary, // vocabulary
      documents.map(_._2.numActives).sum().toLong) // total token count
  }
}