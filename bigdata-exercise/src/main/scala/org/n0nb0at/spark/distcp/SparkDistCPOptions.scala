package org.n0nb0at.spark.distcp

/**
 * Options for the DistCP application
 * See [[OptionsParsing.parse]] for the explanation of each option
 */
case class SparkDistCPOptions(ignoreFailures: Boolean = SparkDistCPOptions.Defaults.ignoreFailures,
                              maxConcurrenceTask: Int = SparkDistCPOptions.Defaults.maxConcurrenceTask) {

  def validateOptions(): Unit = {
    assert(maxConcurrenceTask > 0, "maxConcurrenceTask must be positive")
  }
}

object SparkDistCPOptions {
  object Defaults {
    val ignoreFailures: Boolean = false
    val maxConcurrenceTask: Int = 1000
  }
}
