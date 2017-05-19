package main.scala.obj

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

/**
 * Kho chua du lieu cua LDA
 * @param localDict local dictionary
 * @param docs a list of documents
 * @param M number of documents
 * @param V number of words
 * @param lid2gid map from local coordinates (id) to global ones. Null if the global dictionary is not set
 * @param globalDict link to a global dictionary (optional), null for train data, not null for test data
 */
class LDADataset(var localDict: Dictionary, var docs: ArrayBuffer[Document], var M: Int, var V: Int, var lid2gid: Map[Int, Int], var globalDict: Dictionary) {

  //--------------------------------------------------------------
  // Constructor
  //--------------------------------------------------------------
  def this() = {
    this(new Dictionary, new ArrayBuffer[Document], 0, 0, null, null)
  }

  def this(M: Int) = {
    this(new Dictionary, new ArrayBuffer[Document], M, 0, null, null)
  }

  def this(M: Int, globalDict: Dictionary) = {
    this(new Dictionary, new ArrayBuffer[Document], M, 0, new HashMap[Int, Int], globalDict)
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
      docs.insert(idx, doc)
    }
  }
  /**
   * set the document at the index idx if idx is greater than 0 and less than M
   * @param content string contains doc
   * @param idx index in the document array
   */
  def setDoc(content: String, idx: Int): Unit = {
    //println(content)
    if (0 <= idx && idx < M) {
      var ids = new ArrayBuffer[Int]

      content.split("[ \\t\\n]").foreach(word => {
        var _id = localDict.word2id.size

        if (localDict.contains(word))
          _id = localDict.getId(word)

        if (globalDict != null) {
          //get the global id					
          val id = globalDict.getId(word)
          //println(id)

          if (id != -1) {
            localDict.addWord(word)

            lid2gid.put(_id, id)
            ids.append(_id)
          } else { //not in global dictionary
            //do nothing currently
          }
        } else {
          localDict.addWord(word)
          ids.append(_id)
        }
      })
      
      val doc = new Document(ids, content)
      
      docs.insert(idx, doc)
      V = localDict.word2id.size
    }
  }
}