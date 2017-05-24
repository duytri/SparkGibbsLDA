package main.scala.obj

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * Lop dai dien cho TU DIEN du lieu
 * @param word2id Tu dien tra cuu vi tri
 * @param id2word Tu dien tra cuu noi dung
 */
class Dictionary(var word2id: RDD[(String, Int)], var id2word: RDD[(Int, String)]) {

  //----------------------------------------------------
  // Other constructors
  //----------------------------------------------------
  def this(sc: SparkContext) = {
    this(sc.emptyRDD[(String, Int)], sc.emptyRDD[(Int, String)])
  }

  /**
   * Ham tao tu dien.
   * @param word2id Du lieu lay duoc tu connector.
   */
  def this(sc: SparkContext, word2id: Array[(String, Int)]) = {
    this(sc.parallelize(word2id), sc.parallelize(word2id.map(item => {
      item._2 -> item._1
    })))
  }

  //---------------------------------------------------
  // get/set methods
  //---------------------------------------------------
  /**
   * Ham tra cuu tu tai vi tri (id) trong tu dien.
   * @param id Vi tri can tra cuu tu.
   * @return <li><b><i>null</i></b> neu khong tim thay trong tu dien.
   * <li><b>word</b> neu tim thay trong tu dien.
   */
  def getWord(id: Int): String = {
    val res = id2word.lookup(id)
    if (res.size > 0) return res(0)
    else return null
    /*if (id2word.contains(id))
      id2word.get(id).get
    else null*/
  }

  /**
   * Ham tra cuu vi tri (index) cua tu 'word' trong tu dien.
   * @param word Tu can tra cuu vi tri.
   * @return <li><b>-1</b> neu khong tim thay trong tu dien.
   * <li><b>>=0</b> vi tri cua tu trong tu dien.
   */
  def getId(word: String): Int = {
    val res = word2id.lookup(word)
    if (res.size > 0) return res(0)
    else return -1
  }

  /**
   * Ham tra ve Tu dien tra cuu vi tri
   */
  def getWord2Id() = word2id.collect()

  /**
   * Ham tra ve Tu dien tra cuu noi dung
   */
  def getId2Word() = id2word.collect()

  //----------------------------------------------------
  // checking methods
  //----------------------------------------------------
  /**
   * check if this dictionary contains a specified word
   */
  def contains(word: String): Boolean = {
    val res = word2id.lookup(word)
    if (res.size > 0) return true
    else return false
  }

  /**
   * check if this dictionary contains a specified id
   */
  def contains(id: Int): Boolean = {
    val res = id2word.lookup(id)
    if (res.size > 0) return true
    else return false
  }

  //---------------------------------------------------
  // manupulating methods
  //---------------------------------------------------
  /**
   * Add a word into this dictionary
   * @param word Word need to be added to the dictionary
   * @return The corresponding id
   */
  def addWord(sc: SparkContext, word: String): Int =
    if (!contains(word)) {
      val id: Int = word2id.count().toInt
      word2id = word2id ++ sc.parallelize(List(word -> id), 1)
      id2word = id2word ++ sc.parallelize(List(id -> word), 1)
      id
    } else getId(word)
}