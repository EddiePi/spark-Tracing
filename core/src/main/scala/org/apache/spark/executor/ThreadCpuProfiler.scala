/*
 * class created by Eddie
 *
 */

package org.apache.spark.executor

import java.lang.management.ManagementFactory
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.util.{ThreadUtils, Utils}



class ThreadCpuProfiler (val conf: SparkConf) {
  private val taskIdToThreadId = new ConcurrentHashMap[Long, Long]()

  // these three need to be updated every interval
  private val threadIdToPrevCpuTime = new ConcurrentHashMap[Long, Long]()
  private val taskIdToCpuUsage = new ConcurrentHashMap[Long, Double]()
  private var prevTime: Double = System.currentTimeMillis() * 1000000D

  // used for finished but unreported tasks
  private val unreportedTaskIdToCpuUsage = new ConcurrentHashMap[Long, Double]()

  private val threadMXBean = ManagementFactory.getThreadMXBean
  private val osMXBean = ManagementFactory.getOperatingSystemMXBean
  private val cores = osMXBean.getAvailableProcessors

  private val profileInterval = conf.getTimeAsMs("spark.tracing.cpu.profilingInterval", "3s")

  private val cpuProfileExecutor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("cpu-profile-executor")

  startCpuProfileExecutor()


  // called only after the task is started
  @volatile def registerTask(taskId: Long, threadId: Long): Unit = {
    taskIdToThreadId.put(taskId, threadId)
    threadIdToPrevCpuTime.put(threadId, threadMXBean.getCurrentThreadCpuTime)
    taskIdToCpuUsage.put(taskId, 0D)
  }

  @volatile def unregisterTask(taskId: Long, threadId: Long): Unit = {
    if (taskIdToThreadId.get(taskId) == threadId && threadIdToPrevCpuTime.containsKey(threadId)) {
      unreportedTaskIdToCpuUsage.put(taskId, profileOneTaskCpuUsage(taskId))
    }
    // remove the finished task from these map
    taskIdToThreadId.remove(taskId)
    threadIdToPrevCpuTime.remove(threadId)
    taskIdToCpuUsage.remove(taskId)
  }

  @volatile def getTaskCpuUsage(taskId: Long): Double = {
    if (taskIdToCpuUsage.containsKey(taskId)) {
      taskIdToCpuUsage.get(taskId)
    } else if (unreportedTaskIdToCpuUsage.contains(taskId)) {
      unreportedTaskIdToCpuUsage.remove(taskId)
    } else {
      -1D
    }
  }

  private def profileAllTasksCpuUsage(): Unit = {

    // calculate the cpu usage for each thread
    for (taskId <- taskIdToThreadId.keys()) {
      taskIdToCpuUsage.put(taskId, profileOneTaskCpuUsage(taskId))
    }
  }

  @volatile private def profileOneTaskCpuUsage (taskId: Long): Double = {
    if (taskIdToThreadId.containsKey(taskId)) {
      val threadId = taskIdToThreadId.get(taskId)
      val curTime: Double = System.currentTimeMillis() * 1000000D
      val elapsedTime = curTime - prevTime
      prevTime = curTime
      val curCpuTime =
        threadMXBean.getThreadCpuTime(threadId) - threadIdToPrevCpuTime.get(threadId)
      val cpuUsage: Double =
        Math.min(99D, (curCpuTime - threadIdToPrevCpuTime.get(threadId) / (elapsedTime * cores)))
      taskIdToCpuUsage.put(taskId, cpuUsage)

      // update the previous cpu time for each thread
      threadIdToPrevCpuTime.put(threadId, threadMXBean.getThreadCpuTime(threadId))
      cpuUsage
    } else -1D
  }

  private def startCpuProfileExecutor (): Unit = {
    val intervalMs = conf.getTimeAsMs("spark.tracing.heartbeatInterval", "2s")

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val profileTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(profileAllTasksCpuUsage())
    }
    cpuProfileExecutor.scheduleAtFixedRate(
      profileTask, initialDelay, profileInterval, TimeUnit.MILLISECONDS)
  }
}
