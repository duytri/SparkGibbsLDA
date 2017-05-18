package main.scala.connector

import java.io.BufferedWriter
import java.io.FileWriter
import main.scala.obj.LDADataset
import main.scala.obj.Model
import java.io.File
import main.scala.obj.LDADataset
import scala.collection.mutable.ArrayBuffer

object Model2File {

  /**
   * Save word-topic assignments for this model
   */
  def saveModelTAssign(filename: String, data: LDADataset, z: Array[Array[Int]]): Boolean = {
    val writer = new BufferedWriter(new FileWriter(filename))
    //write docs with topic assignments for words
    for (i <- 0 until data.M) {
      for (j <- 0 until data.docs(i).length) {
        writer.write(data.docs(i).words(j) + ":" + z(i)(j) + " ")
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

    writer.write("alpha=" + alpha + "\n");
    writer.write("beta=" + beta + "\n");
    writer.write("ntopics=" + K + "\n");
    writer.write("ndocs=" + M + "\n");
    writer.write("nwords=" + V + "\n");
    writer.write("liters=" + liter + "\n");

    writer.close
    true
  }

  /**
   * Save model the most likely words for each topic
   * Lay ra twords tu co phan bo xac suat cao nhat theo tung chu de
   */
  def saveModelTwords(filename: String, twords: Int, K: Int, V: Int, phi: Array[Array[Double]], data: LDADataset): Boolean = {
    val writer = new BufferedWriter(new FileWriter(filename))
    val topwords = if (twords > V) V else twords

    for (k <- 0 until K) {
      var wordsProbsList = new ArrayBuffer[(Int, Double)]
      for (w <- 0 until V) {
        wordsProbsList.append((w, phi(k)(w)))
      } //end foreach word

      //print topic				
      writer.write("Topic " + k + "th:\n");
      wordsProbsList.sortWith(_._2 > _._2)

      for (i <- 0 until topwords) {
        if (data.localDict.contains(wordsProbsList(i)._1)) {
          val word = data.localDict.getWord(wordsProbsList(i)._1)

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
  def saveModel(modelName: String, model: Model): Boolean = {
    if (!saveModelTAssign(model.dir + File.separator + "output" + File.separator + model.modelName + model.tassignSuffix, model.data, model.z)) {
      return false
    }

    if (!saveModelOthers(model.dir + File.separator + "output" + File.separator + model.modelName + model.othersSuffix, model.alpha, model.beta, model.K, model.M, model.V, model.liter)) {
      return false
    }

    if (!saveModelTheta(model.dir + File.separator + "output" + File.separator + model.modelName + model.thetaSuffix, model.theta, model.M, model.K)) {
      return false
    }

    if (!saveModelPhi(model.dir + File.separator + "output" + File.separator + model.modelName + model.phiSuffix, model.phi, model.K, model.V)) {
      return false
    }

    if (model.twords > 0) {
      if (!saveModelTwords(model.dir + File.separator + "output" + File.separator + model.modelName + model.twordsSuffix, model.twords, model.K, model.V, model.phi, model.data))
        return false
    }
    return true
  }
}