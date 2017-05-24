package main.scala.helper

import org.apache.spark.util.AccumulatorV2

class TwoDimIntAcc(private var _value: Array[Array[Int]]) extends AccumulatorV2[(Int, Int, Int), Array[Array[Int]]] {

    def this(row: Int, col: Int) { // ham init viet them
      this(Array.ofDim[Int](row, col))
    }
    
    override def add(newValue: (Int, Int, Int)): Unit = {
      _value(newValue._1)(newValue._2) += newValue._3
    }

    def subtract(newValue: (Int, Int, Int)): Unit = {
      _value(newValue._1)(newValue._2) -= newValue._3
    }

    override def copy(): TwoDimIntAcc = {
      new TwoDimIntAcc(value)
    }

    override def isZero(): Boolean = {
      value.flatten.reduce { _ + _ } == 0
    }

    override def merge(other: AccumulatorV2[(Int, Int, Int), Array[Array[Int]]]): Unit = {
      for (i <- 0 until _value.length) { //hang cua mang
        for (j <- 0 until _value(0).length) { // cot cua mang
          _value(i)(j) += other.value.apply(i)(j)
        }
      }
    }

    override def reset(): Unit = {
      _value = Array.ofDim[Int](value.length, value.head.size)
    }

    override def value(): Array[Array[Int]] = {
      _value
    }
  }