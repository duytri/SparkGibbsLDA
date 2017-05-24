package main.scala.helper

import org.apache.spark.util.AccumulatorV2

class OneDimIntAcc(private var _value: Array[Int]) extends AccumulatorV2[(Int, Int), Array[Int]] {

  def this(length: Int) { // ham init viet them
    this(Array.ofDim[Int](length))
  }

  override def add(newValue: (Int, Int)): Unit = {
    _value(newValue._1) += newValue._2
  }

  def subtract(newValue: (Int, Int)): Unit = {
    _value(newValue._1) -= newValue._2
  }

  override def copy(): OneDimIntAcc = {
    new OneDimIntAcc(value)
  }

  override def isZero(): Boolean = {
    value.reduce { _ + _ } == 0
  }

  override def merge(other: AccumulatorV2[(Int, Int), Array[Int]]): Unit = {
    for (i <- 0 until _value.length) { //hang cua mang
      _value(i) += other.value.apply(i)
    }
  }

  override def reset(): Unit = {
    _value = Array.ofDim[Int](value.length)
  }

  override def value(): Array[Int] = {
    _value
  }
}