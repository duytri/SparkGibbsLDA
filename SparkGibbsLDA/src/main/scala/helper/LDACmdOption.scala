package main.scala.helper

import org.apache.commons.cli.Options
import org.apache.commons.cli.Option
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.BasicParser


object LDACmdOption {

  var options = new Options

  var help = new Option("h", "help", true, "Print this message")
  help.setRequired(false)
  help.setArgs(1)
  options.addOption(help)

  var estimate = new Option("est", "estimate", true, "Specify whether we want to estimate model from scratch")
  estimate.setRequired(false)
  options.addOption(estimate)
  var est = false

  var estcontinue = new Option("estc", "estcontinue", true, "Specify whether we want to continue the last estimation")
  estcontinue.setRequired(false)
  options.addOption(estcontinue)
  //@Option(name = "-estc", usage = "Specify whether we want to continue the last estimation")
  var estc = false

  var inference = new Option("inf", "inference", true, "Specify whether we want to do inference")
  inference.setRequired(false)
  options.addOption(inference)
  //@Option(name = "-inf", usage = "Specify whether we want to do inference")
  var inf = true

  var directory = new Option("dir", "directory", true, "Specify directory")
  directory.hasArg()
  directory.setRequired(true)
  options.addOption(directory)
  //@Option(name = "-dir", usage = "Specify directory")
  var dir = ""

  var datafile = new Option("dfile", "datafile", true, "Specify data file")
  datafile.hasArg()
  datafile.setRequired(true)
  options.addOption(datafile)
  //@Option(name = "-dfile", usage = "Specify data file")
  var dfile = ""

  var modelname = new Option("model", "modelname", true, "Specify the model name")
  modelname.hasArg()
  modelname.setRequired(true)
  options.addOption(modelname)
  //@Option(name = "-model", usage = "Specify the model name")
  var modelName = ""

  var al = new Option("a", "alpha", true, "Specify alpha")
  al.hasArg()
  al.setRequired(true)
  options.addOption(al)
  //@Option(name = "-alpha", usage = "Specify alpha")
  var alpha = -1.0

  var bet = new Option("b", "beta", true, "Specify beta")
  bet.hasArg()
  bet.setRequired(true)
  options.addOption(bet)
  //@Option(name = "-beta", usage = "Specify beta")
  var beta = -1.0

  var ntopics = new Option("K", "ntopics", true, "Specify the number of topics")
  ntopics.hasArg()
  ntopics.setRequired(true)
  options.addOption(ntopics)
  //@Option(name = "-ntopics", usage = "Specify the number of topics")
  var K = 100

  var ni = new Option("ni", "niters", true, "Specify the number of iterations")
  ni.hasArg()
  ni.setRequired(true)
  options.addOption(ni)
  //@Option(name = "-niters", usage = "Specify the number of iterations")
  var niters = 1000

  var ss = new Option("ss", "savestep", true, "Specify the number of steps to save the model since the last save")
  ss.hasArg()
  ss.setRequired(false)
  options.addOption(ss)
  //@Option(name = "-savestep", usage = "Specify the number of steps to save the model since the last save")
  var savestep = 100

  var tw = new Option("tw", "twords", true, "Specify the number of most likely words to be printed for each topic")
  tw.hasArg()
  tw.setRequired(false)
  options.addOption(tw)
  //@Option(name = "-twords", usage = "Specify the number of most likely words to be printed for each topic")
  var twords = 100

  var wd = new Option("wd", "withrawdata", true, "Specify whether we include raw data in the input")
  wd.hasArg()
  wd.setRequired(false)
  options.addOption(wd)
  //@Option(name = "-withrawdata", usage = "Specify whether we include raw data in the input")
  var withrawdata = false

  var wordmap = new Option("wm", "wordmap", true, "Specify the wordmap file")
  wordmap.hasArg()
  wordmap.setRequired(false)
  options.addOption(wordmap)
  //@Option(name = "-wordmap", usage = "Specify the wordmap file")
  var wordMapFileName = "wordmap.txt";

  val parser: CommandLineParser = new BasicParser
  val formatter = new HelpFormatter

  def showHelp(): Unit = {
    formatter.printHelp("sparkgibbslda.jar [--options] [args...]", options)
  }

  def getArguments(args: Array[String]): CommandLine = {
    parser.parse(options, args)
  }

}