package main.scala.obj

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * Kho chua du lieu cua LDA
 * @param localDict local dictionary
 * @param docs a list of documents
 * @param M number of documents
 * @param V number of words
 * @param lid2gid map from local coordinates (id) to global ones. Null if the global dictionary is not set
 * @param globalDict link to a global dictionary (optional), null for train data, not null for test data
 */
class LDADataset(val sc: SparkContext, var localDict: Dictionary, var docs: RDD[Document], var M: Int, var V: Int) {

  //--------------------------------------------------------------
  // Constructor
  //--------------------------------------------------------------
  def this(sc: SparkContext) = {
    this(sc, new Dictionary(sc), sc.emptyRDD[Document], 0, 0)
  }

  def this(sc: SparkContext, M: Int) = {
    this(sc, new Dictionary(sc), sc.emptyRDD[Document], M, 0)
  }

  //-------------------------------------------------------------
  //Public Instance Methods
  //-------------------------------------------------------------
  /**
   * set the document at the index idx if idx is greater than 0 and less than M
   * @param doc document to be set
   * @param idx index in the document array
   */
  def setDoc(doc: Document, idx: Int): Unit = {
    if (0 <= idx && idx < M) {
      docs = docs ++ sc.parallelize(List(doc))
    }
  }
}