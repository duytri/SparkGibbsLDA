package main.scala.obj

import main.java.commons.cli.CommandLine
import java.io.File

class Parameter {

  var alpha: Double = -1.0
  var beta: Double = -1.0
  var K: Int = -1
  var niters: Int = -1
  var directory: String = "@"
  //var datafile: String = "@"
  var modelname: String = "@"
  var twords: Int = -1
  var wordMapFileName: String = "@"

  def getParams(cmd: CommandLine): Unit = {
    alpha = if (cmd.hasOption("alpha")) cmd.getOptionValue("alpha").toDouble else -1.0
    beta = if (cmd.hasOption("beta")) cmd.getOptionValue("beta").toDouble else -1.0
    K = if (cmd.hasOption("ntopics")) cmd.getOptionValue("ntopics").toInt else -1
    niters = if (cmd.hasOption("niters")) cmd.getOptionValue("niters").toInt else -1
    //datafile = if (cmd.hasOption("datafile")) cmd.getOptionValue("datafile") else "@"
    modelname = if (cmd.hasOption("modelname")) cmd.getOptionValue("modelname") else "@"
    //savestep = if (cmd.hasOption("savestep")) cmd.getOptionValue("savestep").toInt else -1
    twords = if (cmd.hasOption("twords")) cmd.getOptionValue("twords").toInt else -1
    //withrawdata = if (cmd.hasOption("withrawdata")) true else false
    wordMapFileName = if (cmd.hasOption("wordmap")) cmd.getOptionValue("wordmap") else "@"
    directory = {
      if (cmd.hasOption("directory")) {
        var dir = cmd.getOptionValue("directory")
        if (dir.last.toString == File.separator)
          dir.substring(0, dir.length - 1)
        else dir
      } else "@"
    }
  }

  def checkRequirement(): Boolean = {
    if (alpha < 0.0d || beta < 0.0d || K < 0 || niters < 0 || directory.compareTo("@") == 0)
      false
    else true
  }
}