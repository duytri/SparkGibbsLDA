package main.scala.helper

object Conversion {
  def zeroPad(number: Int, width: Int): String = {
    val result: StringBuffer = new StringBuffer("")
    for (i <- 0 until width - number.toString.length)
      result.append("0")
    result.append(number)
    result.toString
  }
}