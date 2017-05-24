package main.scala.obj

import scala.collection.mutable.ArrayBuffer

/**
 * Lop dai dien cho mot TAI LIEU
 * @param wordIndexes Chua cac tu (index trong tu dien) cua mot tai lieu (van ban)
 * @param rawStr Noi dung tho cua tai lieu
 * @param length So tu trong tai lieu do
 */
class Document(var wordIndexes: ArrayBuffer[Int], var rawStr: String, var length: Int) {
  
  //----------------------------------------------------
	// Other constructors
	//----------------------------------------------------
  def this() = {
    this(null, "", 0)
  }
  
  def this(length: Int) = {
    this(new ArrayBuffer[Int], "", length)
  }

  def this(length: Int, words: ArrayBuffer[Int]) = {
    this(words, "", length)
  }

  def this(doc: ArrayBuffer[Int]) = {
    this(doc, "", doc.size)
  }

  def this(doc: ArrayBuffer[Int], rawStr: String) = {
    this(doc, rawStr, doc.size)
  }
}