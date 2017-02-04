
package org.apache.spark.executor

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.util.{ThreadUtils, Utils}


class TaskMemoryProfiler (env: SparkEnv) extends Logging {
  val conf = env.conf
  val memoryManager = env.memoryManager

  val taskIdToManager = new ConcurrentHashMap[Long, TaskMemoryManager]
  val taskIdToExecMemory = new ConcurrentHashMap[Long, Long]
  val taskIdToStoreMemory = new ConcurrentHashMap[Long, Long]

  val unreportedTaskIdToExecMemory = new ConcurrentHashMap[Long, Long]
  val unreportedTaskIdToStoreMemory = new ConcurrentHashMap[Long, Long]

  private val profileInterval = conf.getTimeAsMs("spark.tracing.profilingInterval", "3s")

  private val memoryProfileThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("cpu-profile-executor")

  @volatile def registerTask (taskId: Long, taskMemoryManager: TaskMemoryManager): Unit = {
    if (!taskIdToManager.contains(taskId)) {
      taskIdToManager.put(taskId, taskMemoryManager)
    }
  }

  @volatile def unregisterTask (taskId: Long): Unit = {
    if (taskIdToManager.containsKey(taskId)) {
      taskIdToManager.remove(taskId)
      taskIdToExecMemory.remove(taskId)
      taskIdToStoreMemory.remove(taskId)

      // TODO: Profile again here
      val (execMem, storeMem) = profileOneTaskMemory(taskId)
      unreportedTaskIdToExecMemory.put(taskId, execMem)
      unreportedTaskIdToStoreMemory.put(taskId, storeMem)
    }
  }

  private def profileAllTasksMemoryUsage(): Unit = {

  }

  @volatile private def profileOneTaskMemory (taskId: Long): (Long, Long) = {
    (-1L, -1L)
  }

  private def profileTaskExecMemory(taskId: Long): Long = {
    val taskMemoryManager = taskIdToManager.get(taskId)
    return taskMemoryManager.getMemoryConsumptionForThisTask
  }

  private[executor] def start(): Unit = {
    val intervalMs = profileInterval

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val profileTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(profileAllTasksMemoryUsage())
    }
    memoryProfileThread.scheduleAtFixedRate(
      profileTask, initialDelay, profileInterval, TimeUnit.MILLISECONDS)
  }
}
