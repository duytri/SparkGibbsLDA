package main.scala.obj

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

/**
 * Lop dai dien cho TU DIEN du lieu
 * @param word2id Tu dien tra cuu vi tri
 * @param id2word Tu dien tra cuu noi dung
 */
class Dictionary(var word2id: Map[String, Int], var id2word: Map[Int, String]) {

  //----------------------------------------------------
  // Other constructors
  //----------------------------------------------------
  def this() = {
    this(new HashMap[String, Int], new HashMap[Int, String])
  }

  /**
   * Ham tao tu dien.
   * @param word2id Du lieu lay duoc tu connector.
   */
  def this(word2id: Map[String, Int]) = {
    this(word2id, word2id.map(item => {
      item._2 -> item._1
    }))
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
    if (id2word.contains(id))
      id2word.get(id).get
    else null
  }

  /**
   * Ham tra cuu vi tri (index) cua tu 'word' trong tu dien.
   * @param word Tu can tra cuu vi tri.
   * @return <li><b>-1</b> neu khong tim thay trong tu dien.
   * <li><b>>=0</b> vi tri cua tu trong tu dien.
   */
  def getId(word: String): Int = {
    if (word2id.contains(word))
      word2id.get(word).get
    else -1
  }

  /**
   * Ham tra ve Tu dien tra cuu vi tri
   */
  def getWord2Id() = word2id

  /**
   * Ham tra ve Tu dien tra cuu noi dung
   */
  def getId2Word() = id2word

  //----------------------------------------------------
  // checking methods
  //----------------------------------------------------
  /**
   * check if this dictionary contains a specified word
   */
  def contains(word: String): Boolean = word2id.contains(word)

  /**
   * check if this dictionary contains a specified id
   */
  def contains(id: Int): Boolean = id2word.contains(id)

  //---------------------------------------------------
  // manupulating methods
  //---------------------------------------------------
  /**
   * Add a word into this dictionary
   * @param word Word need to be added to the dictionary
   * @return The corresponding id
   */
  def addWord(word: String): Int =
    if (!contains(word)) {
      val id: Int = word2id.size
      word2id.put(word, id)
      id2word.put(id, word)
      id
    } else getId(word)
}