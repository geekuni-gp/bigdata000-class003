package org.n0nb0at.spark.distcp

import org.apache.spark.sql.SparkSession

object DistCp {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().getOrCreate()

    val config = OptionsParsing.parse(args, sparkSession.sparkContext.hadoopConfiguration)

    val (src, dest) = config.sourceAndDestPaths
  }

  def checkDir(sourcePath: String, targetPath: String): Unit = {

  }
}