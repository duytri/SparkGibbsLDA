package main.scala.helper

import org.apache.spark.util.AccumulatorV2

class ThreeDimAccumulator(private var _value: Array[Array[Array[Int]]]) extends AccumulatorV2[(Int, Int, Int, Int), Array[Array[Array[Int]]]] {

  def this(x: Int, y: Int, z: Int) { // ham init viet them
    this(Array.ofDim[Int](x, y, z))
  }

  override def add(newValue: (Int, Int, Int, Int)): Unit = {
    _value(newValue._1)(newValue._2)(newValue._3) += newValue._4
  }

  def subtract(newValue: (Int, Int, Int, Int)): Unit = {
    _value(newValue._1)(newValue._2)(newValue._3) -= newValue._4
  }

  override def copy(): ThreeDimAccumulator = {
    new ThreeDimAccumulator(value)
  }

  override def isZero(): Boolean = {
    value.flatten.flatten.reduce { _ + _ } == 0
  }

  override def merge(other: AccumulatorV2[(Int, Int, Int, Int), Array[Array[Array[Int]]]]): Unit = {
    for (x <- 0 until _value.length) { // x-dim
      for (y <- 0 until _value(0).length) { // y-dim
        for (z <- 0 until _value(0)(0).length) { // z-dim
          _value(x)(y)(z) += other.value.apply(x)(y)(z)
        }
      }
    }
  }

  override def reset(): Unit = {
    _value = Array.ofDim[Int](value.length, value.head.length, value.head.head.length)
  }

  override def value(): Array[Array[Array[Int]]] = {
    _value
  }
}