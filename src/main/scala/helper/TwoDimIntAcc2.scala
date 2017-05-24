package main.scala.helper

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.ArrayBuffer

class TwoDimIntAcc2(private var _value: Array[ArrayBuffer[Int]]) extends AccumulatorV2[(Int, Int, Int), Array[ArrayBuffer[Int]]] {

  def this(dim1: Int) { // ham init viet them
    this(Array.ofDim[ArrayBuffer[Int]](dim1))
  }

  def initSecondDim(dim1: Int, dim2: Int) = {
    _value(dim1) = new ArrayBuffer[Int](dim2)
    for (i <- 0 until dim2) {
      _value(dim1).insert(i, 0)
    }
  }

  def setValue(newValue: (Int, Int, Int)): Unit = {
    _value(newValue._1).update(newValue._2, newValue._3)
  }

  override def add(newValue: (Int, Int, Int)): Unit = {
    _value(newValue._1)(newValue._2) += newValue._3
  }

  def subtract(newValue: (Int, Int, Int)): Unit = {
    _value(newValue._1)(newValue._2) -= newValue._3
  }

  override def copy(): TwoDimIntAcc2 = {
    new TwoDimIntAcc2(value)
  }

  override def isZero(): Boolean = {
    value.apply(0) == null
  }

  override def merge(other: AccumulatorV2[(Int, Int, Int), Array[ArrayBuffer[Int]]]): Unit = {
    for (i <- 0 until _value.length) {
      if (other.value(i) != null) {
        if (_value(i) != null) {
          for (j <- 0 until _value(i).length) {
            _value(i)(j) += other.value(i)(j)
          }
        } else {
          _value(i) = other.value(i)
        }
      }
    }
  }

  override def reset(): Unit = {
    _value = Array.ofDim[ArrayBuffer[Int]](value.length)
  }

  override def value(): Array[ArrayBuffer[Int]] = {
    _value
  }
}