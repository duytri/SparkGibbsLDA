package main.scala.helper

import scala.collection.mutable.ArrayBuffer
import main.scala.obj.LDADataset

object Utils {
  def printTopWords(topic: Int, vocabSize: Int, phi: Array[Array[Double]], data: LDADataset, top: Int): Unit = {
    var wordsProbsList = new ArrayBuffer[(Int, Double)]
    for (w <- 0 until vocabSize) {
      wordsProbsList.append((w, phi(topic)(w)))
    } //end foreach word
    wordsProbsList = wordsProbsList.sortWith(_._2 > _._2)

    for (i <- 0 until top) {
      if (data.localDict.contains(wordsProbsList(i)._1)) {
        println("\t" + data.localDict.getWord(wordsProbsList(i)._1) + " " + wordsProbsList(i)._2);
      }
    }
  }
}