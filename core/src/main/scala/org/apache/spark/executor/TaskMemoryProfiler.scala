
package org.apache.spark.executor

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging


class TaskMemoryProfiler (conf: SparkConf) extends Logging {
  def registerTask (taskId: Long): Unit = {}

  def unregisterTask (taskId: Long): Unit = {}

}
