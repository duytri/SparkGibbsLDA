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

  val directory = new Option("d", "directory", true, "Specify HDFS directory")
  directory.setRequired(false)
  directory.setArgName("folder")
  options.addOption(directory)
  
  val output = new Option("o", "output", true, "Specify output directory")
  output.setRequired(false)
  output.setArgName("folder")
  options.addOption(output)

  val modelname = new Option("m", "modelname", true, "Specify the model name")
  modelname.setRequired(false)
  modelname.setArgName("name")
  options.addOption(modelname)

  val al = new Option("a", "alpha", true, "Specify alpha")
  al.setRequired(false)
  al.setArgName("double")
  options.addOption(al)

  val bet = new Option("b", "beta", true, "Specify beta")
  bet.setRequired(false)
  bet.setArgName("double")
  options.addOption(bet)

  val ntopics = new Option("K", "ntopics", true, "Specify the number of topics")
  ntopics.setRequired(false)
  ntopics.setArgName("number")
  options.addOption(ntopics)

  val ni = new Option("n", "niters", true, "Specify the number of iterations")
  ni.setRequired(false)
  ni.setArgName("number")
  options.addOption(ni)

  val tw = new Option("t", "twords", true, "Specify the number of most likely words to be printed for each topic")
  tw.setRequired(false)
  tw.setArgName("number")
  options.addOption(tw)
  
  val wordmap = new Option("wm", "wordmap", true, "Specify the wordmap file")
  wordmap.setRequired(false)
  wordmap.setArgName("file")
  options.addOption(wordmap)

  val parser: CommandLineParser = new DefaultParser
  val formatter = new HelpFormatter

  def showHelp(): Unit = {
    formatter.printHelp("sparkgibbslda.jar [--options] [args...]", options)
  }

  def getArguments(args: Array[String]): CommandLine = {
    parser.parse(options, args)
  }

}