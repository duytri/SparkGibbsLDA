package main.scala.connector

import java.io.BufferedWriter
import java.io.FileWriter
import java.io.File
import scala.collection.mutable.ArrayBuffer
import main.scala.obj.Document

object Model2File {

  /**
   * Save word-topic assignments for this model
   */
  def saveModelTAssign(filename: String, docs: Array[(Document, Long)], z: Array[ArrayBuffer[Int]]): Boolean = {
    val writer = new BufferedWriter(new FileWriter(filename))
    writer.flush
    //write docs with topic assignments for words
    for (i <- 0 until docs.length) {
      for (j <- 0 until docs(i)._1.length) {
        writer.write(docs(i)._1.wordIndexes(j) + ":" + z(docs(i)._2.toInt)(j) + " ")
      }
      writer.write("\n")
    }
    writer.close
    true
  }

  /**
   * Save theta (topic distribution) for this model
   */
  def saveModelTheta(filename: String, theta: Array[Array[Double]], M: Int, K: Int): Boolean = {
    val writer = new BufferedWriter(new FileWriter(filename))
    writer.flush
    for (i <- 0 until M) {
      for (j <- 0 until K) {
        writer.write(theta(i)(j) + " ");
      }
      writer.write("\n");
    }
    writer.close
    true
  }

  /**
   * Save word-topic distribution
   */
  def saveModelPhi(filename: String, phi: Array[Array[Double]], K: Int, V: Int): Boolean = {
    val writer = new BufferedWriter(new FileWriter(filename))
    writer.flush
    for (i <- 0 until K) {
      for (j <- 0 until V) {
        writer.write(phi(i)(j) + " ");
      }
      writer.write("\n");
    }
    writer.close
    true
  }

  /**
   * Save other information of this model
   */
  def saveModelOthers(filename: String, alpha: Double, beta: Double, K: Int, M: Int, V: Int, liter: Int): Boolean = {
    val writer = new BufferedWriter(new FileWriter(filename))
    writer.flush

    writer.write("alpha=" + alpha + "\n");
    writer.write("beta=" + beta + "\n");
    writer.write("ntopics=" + K + "\n");
    writer.write("ndocs=" + M + "\n");
    writer.write("nwords=" + V + "\n");
    writer.write("liter=" + liter + "\n");

    writer.close
    true
  }

  /**
   * Save model the most likely words for each topic
   * Lay ra twords tu co phan bo xac suat cao nhat theo tung chu de
   */
  def saveModelTwords(filename: String, twords: Int, K: Int, V: Int, phi: Array[Array[Double]], id2word: Map[Int, String]): Boolean = {
    val writer = new BufferedWriter(new FileWriter(filename))
    writer.flush
    val topwords = if (twords > V) V else twords

    for (k <- 0 until K) {
      var wordsProbsList = new ArrayBuffer[(Int, Double)]
      for (w <- 0 until V) {
        wordsProbsList.append((w, phi(k)(w)))
      } //end foreach word

      //print topic				
      writer.write("Topic " + k + "th:\n");
      wordsProbsList = wordsProbsList.sortWith(_._2 > _._2)

      for (i <- 0 until topwords) {
        if (id2word.contains(wordsProbsList(i)._1)) {
          val word = id2word.get(wordsProbsList(i)._1).get

          writer.write("\t" + word + " " + wordsProbsList(i)._2 + "\n");
        }
      }
    } //end foreach topic			

    writer.close
    true
  }

  /**
   * Save model
   */
  def saveModel(outputDir: String, modelName: String, alpha: Double, beta: Double, K: Int, M: Int, V: Int, twords: Int, liter: Int, docs: Array[(Document, Long)], z: Array[ArrayBuffer[Int]], theta: Array[Array[Double]], phi: Array[Array[Double]], id2word: Map[Int, String]): Boolean = {
    val tassignSuffix = ".tassign"
    val othersSuffix = ".others"
    val thetaSuffix = ".theta"
    val phiSuffix = ".phi"
    val twordsSuffix = ".twords"
    if (!saveModelTAssign(outputDir + File.separator + modelName + tassignSuffix, docs, z)) {
      return false
    }

    if (!saveModelOthers(outputDir + File.separator + modelName + othersSuffix, alpha, beta, K, M, V, liter)) {
      return false
    }

    if (!saveModelTheta(outputDir + File.separator + modelName + thetaSuffix, theta, M, K)) {
      return false
    }

    if (!saveModelPhi(outputDir + File.separator + modelName + phiSuffix, phi, K, V)) {
      return false
    }

    if (twords > 0) {
      if (!saveModelTwords(outputDir + File.separator + modelName + twordsSuffix, twords, K, V, phi, id2word))
        return false
    }
    return true
  }
}