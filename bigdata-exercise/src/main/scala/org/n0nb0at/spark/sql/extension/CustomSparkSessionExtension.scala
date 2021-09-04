package org.n0nb0at.spark.sql.extension

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

class CustomSparkSessionExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
      logWarning("进入自定义扩展")
      CustomRule(session)
    }
  }
}
