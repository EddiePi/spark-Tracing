
package org.apache.spark.executor

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import org.apache.spark.{SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.Task
import org.apache.spark.tracing.TaskInfo
import org.apache.spark.util.{ThreadUtils, Utils}

import scala.collection.mutable


private[executor] class TaskProfileManager (val env: SparkEnv) extends Logging {
  val conf = env.conf
  val tracingManager = env.tracingManager

  val runningTasks = new ConcurrentHashMap[Long, TaskInfo]
  val unreportedTasks = new ConcurrentHashMap[Long, TaskInfo]

  val taskCpuProfiler: TaskCpuProfiler = new TaskCpuProfiler(conf)

  // TODO: this profiler is under construction
  val taskMemoryProfiler: TaskMemoryProfiler = new TaskMemoryProfiler(conf)

  taskCpuProfiler.start()

  // Edit by Eddie
  // Tracing heartbeat
  private val tracingHeartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("tracing-heartbeater")

  @volatile def registerTask(taskId: Long, task: Task[Any], threadId: Long): Unit = {
    if (!runningTasks.contains(taskId)) {
      runningTasks.put(taskId, new TaskInfo(
        taskId,
        task.stageId,
        task.stageAttemptId,
        task.jobId.getOrElse(-1),
        task.appId.getOrElse("anonymous-app"),
        System.currentTimeMillis(),
        -1L,
        0.0D,
        0L,
        "RUNNING"
      ))
      taskCpuProfiler.registerTask(taskId, threadId)
      taskMemoryProfiler.registerTask(taskId)
    }
  }

  /*
    update finished task information here
   */
  @volatile def unregisterTask(taskId: Long, status: String): Unit = {
    if (runningTasks.containsKey(taskId)) {
      val taskInfo = runningTasks.get(taskId)
      runningTasks.remove(taskId)

      // First we unregister the task from the profiler.
      // in the unregister process the profiler will also mark the task as finished.
      taskCpuProfiler.unregisterTask(taskId)
      taskMemoryProfiler.unregisterTask(taskId)
      // update taskInfo when task finished.
      taskInfo.finishTime = System.currentTimeMillis()
      taskInfo.status = status
      taskInfo.cpuUsage = taskCpuProfiler.getTaskCpuUsage(taskId)
      // TODO: update memory usage

      if (!unreportedTasks.contains(taskId)) {
        unreportedTasks.put(taskId, taskInfo)
      }
    }
  }

  // Edit by Eddie
  /**
    * collect and prepare the task tracing information
    */
  private def prepareRunningTaskTracingInfo(): mutable.Set[TaskInfo] = {
    val taskSet: mutable.Set[TaskInfo] = new mutable.HashSet[TaskInfo]()
    val valueIterator = runningTasks.values().iterator()
    while (valueIterator.hasNext) {
      taskSet.add(valueIterator.next())
    }
    taskSet
  }

  // Edit by Eddie
  private def reportTracingHeartbeat(): Unit = {
    val taskSet = prepareRunningTaskTracingInfo()
    for (taskInfo <- taskSet) {
      logDebug("reporting tracing heartbeat. Size of taskSet is: " + taskSet.size)
      tracingManager.createOrUpdateTaskInfo(taskInfo)
    }
  }

  // Edit by Eddie
  private[executor] def startTracingHeartbeater(): Unit = {
    val intervalMs = conf.getTimeAsMs("spark.tracing.heartbeatInterval", "2s")

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportTracingHeartbeat())
    }
    tracingHeartbeater.scheduleAtFixedRate(
      heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }
}
