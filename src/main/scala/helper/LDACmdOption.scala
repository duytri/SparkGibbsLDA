package main.scala.helper

import main.java.commons.cli.Options
import main.java.commons.cli.Option
import main.java.commons.cli.CommandLineParser
import main.java.commons.cli.HelpFormatter
import main.java.commons.cli.CommandLine
import main.java.commons.cli.DefaultParser


object LDACmdOption {

  var options = new Options

  val help = new Option("h", "help", true, "Print this help message")
  help.setRequired(false)
  help.setArgs(0)
  options.addOption(help)

  /*val estimate = new Option("e", "estimate", true, "Specify whether we want to estimate model from scratch")
  estimate.setRequired(false)
  estimate.setArgs(0)
  options.addOption(estimate)*/
  //var est = false

  /*val estcontinue = new Option("ec", "estcon", true, "Specify whether we want to continue the last estimation")
  estcontinue.setRequired(false)
  estcontinue.setArgs(0)
  options.addOption(estcontinue)*/
  //@Option(name = "-estc", usage = "Specify whether we want to continue the last estimation")
  //var estc = false

  /*val inference = new Option("i", "inference", true, "Specify whether we want to do inference")
  inference.setRequired(false)
  inference.setArgs(0)
  options.addOption(inference)*/
  //@Option(name = "-inf", usage = "Specify whether we want to do inference")
  //var inf = true

  val directory = new Option("d", "directory", true, "Specify HDFS directory")
  directory.setRequired(false)
  directory.setArgName("folder")
  options.addOption(directory)
  //@Option(name = "-dir", usage = "Specify directory")
  //var dir = ""

  /*val datafile = new Option("df", "datafile", true, "Specify data file")
  datafile.setRequired(false)
  datafile.setArgName("file")  
  options.addOption(datafile)*/
  //@Option(name = "-dfile", usage = "Specify data file")
  //var dfile = ""

  val modelname = new Option("m", "modelname", true, "Specify the model name")
  modelname.setRequired(false)
  modelname.setArgName("name")
  options.addOption(modelname)
  //@Option(name = "-model", usage = "Specify the model name")
  //var modelName = ""

  val al = new Option("a", "alpha", true, "Specify alpha")
  al.setRequired(false)
  al.setArgName("double")
  options.addOption(al)
  //@Option(name = "-alpha", usage = "Specify alpha")
  //var alpha = -1.0

  val bet = new Option("b", "beta", true, "Specify beta")
  bet.setRequired(false)
  bet.setArgName("double")
  options.addOption(bet)
  //@Option(name = "-beta", usage = "Specify beta")
  //var beta = -1.0

  val ntopics = new Option("K", "ntopics", true, "Specify the number of topics")
  ntopics.setRequired(false)
  ntopics.setArgName("number")
  options.addOption(ntopics)
  //@Option(name = "-ntopics", usage = "Specify the number of topics")
  //var K = 100

  val ni = new Option("n", "niters", true, "Specify the number of iterations")
  ni.setRequired(false)
  ni.setArgName("number")
  options.addOption(ni)
  //@Option(name = "-niters", usage = "Specify the number of iterations")
  //var niters = 1000

  /*val ss = new Option("s", "savestep", true, "Specify the number of steps to save the model since the last save")
  ss.setRequired(false)
  ss.setArgName("number")
  options.addOption(ss)*/
  //@Option(name = "-savestep", usage = "Specify the number of steps to save the model since the last save")
  //var savestep = 100

  val tw = new Option("t", "twords", true, "Specify the number of most likely words to be printed for each topic")
  tw.setRequired(false)
  tw.setArgName("number")
  options.addOption(tw)
  //@Option(name = "-twords", usage = "Specify the number of most likely words to be printed for each topic")
  //var twords = 100

  /*val wd = new Option("wd", "withrawdata", true, "Specify whether we include raw data in the input")
  wd.setRequired(false)
  wd.setArgs(0)
  options.addOption(wd)*/
  //@Option(name = "-withrawdata", usage = "Specify whether we include raw data in the input")
  //var withrawdata = false

  val wordmap = new Option("wm", "wordmap", true, "Specify the wordmap file")
  wordmap.setRequired(false)
  wordmap.setArgName("file")
  options.addOption(wordmap)
  //@Option(name = "-wordmap", usage = "Specify the wordmap file")
  //var wordMapFileName = "wordmap.txt";

  val parser: CommandLineParser = new DefaultParser
  val formatter = new HelpFormatter

  def showHelp(): Unit = {
    formatter.printHelp("sparkgibbslda.jar [--options] [args...]", options)
  }

  def getArguments(args: Array[String]): CommandLine = {
    parser.parse(options, args)
  }

}