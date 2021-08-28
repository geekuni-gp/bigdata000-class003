package org.n0nb0at.spark.distcp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.net.URI

object OptionsParsing {

  /**
   * Parse a set of command-line arguments into a [[Config]] object
   */
  def parse(args: Array[String], hadoopConfiguration: Configuration): Config = {
    val builder = scopt.OParser.builder[Config]
    val parser = {
      import builder._
      scopt.OParser.sequence(
        opt[Unit]("i")
          .action((_, c) => c.copyOptions(_.copy(ignoreFailures = true)))
          .optional()
          .text("Ignore failures"),

        opt[Int]("m")
          .action((i, c) => c.copyOptions(_.copy(maxConcurrenceTask = i)))
          .optional()
          .text("Maximum number of files to copy in a single Spark task"),

        help("help").text("prints this usage text").optional(),

        arg[String]("[source_path...] <target_path>")
          .unbounded()
          .minOccurs(1)
          .action((u, c) => c.copy(URIs = c.URIs :+ new URI(u)))
      )
    }

    scopt.OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        config.options.validateOptions()
        config
      case _ =>
        throw new RuntimeException("Failed to parse arguments")
    }
  }
}

case class Config(options: SparkDistCPOptions = SparkDistCPOptions(), URIs: Seq[URI] = Seq.empty) {

  def copyOptions(f: SparkDistCPOptions => SparkDistCPOptions): Config = {
    this.copy(options = f(options))
  }

  def sourceAndDestPaths: (Seq[Path], Path) = {
    URIs.reverse match {
      case d :: s :: ts => ((s :: ts).reverse.map(u => new Path(u)), new Path(d))
      case _ => throw new RuntimeException("Incorrect number of URIs")
    }
  }
}