package main.scala.helper

import org.apache.spark.util.AccumulatorV2

class OneDimDoubleAcc(private var _value: Array[Double]) extends AccumulatorV2[(Int, Double), Array[Double]] {

  def this(length: Int) { // ham init viet them
    this(Array.ofDim[Double](length))
  }

  override def add(newValue: (Int, Double)): Unit = {
    _value(newValue._1) += newValue._2
  }
  def subtract(newValue: (Int, Double)): Unit = {
    _value(newValue._1) -= newValue._2
  }

  def set(newValue: (Int, Double)): Unit = {
    _value(newValue._1) = newValue._2
  }

  override def copy(): OneDimDoubleAcc = {
    new OneDimDoubleAcc(value)
  }

  override def isZero(): Boolean = {
    value.reduce { _ + _ } == 0
  }

  override def merge(other: AccumulatorV2[(Int, Double), Array[Double]]): Unit = {
    for (i <- 0 until _value.length) { //hang cua mang
      _value(i) += other.value.apply(i)
    }
  }

  override def reset(): Unit = {
    _value = Array.ofDim[Double](value.length)
  }

  override def value(): Array[Double] = {
    _value
  }
}