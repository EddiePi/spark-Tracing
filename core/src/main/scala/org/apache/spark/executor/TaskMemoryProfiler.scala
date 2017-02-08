
package org.apache.spark.executor

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.util.{ThreadUtils, Utils}

 /*
  * only periodically profile execution memory
  * storage memory is set when job about to write
  *
  */
class TaskMemoryProfiler (env: SparkEnv) extends Logging {
   val conf = env.conf
   val memoryManager = env.memoryManager

   val taskIdToManager = new ConcurrentHashMap[Long, TaskMemoryManager]
   val taskIdToExecMemory = new ConcurrentHashMap[Long, Long]
   val taskIdToStoreMemory = new ConcurrentHashMap[Long, Long]

   val unreportedTaskIdToExecMemory = new ConcurrentHashMap[Long, Long]
   // val unreportedTaskIdToStoreMemory = new ConcurrentHashMap[Long, Long]

   private val profileInterval = conf.getTimeAsMs("spark.tracing.profilingInterval", "3s")

   private val memoryProfileThread =
     ThreadUtils.newDaemonSingleThreadScheduledExecutor("cpu-profile-executor")

   @volatile def registerTask(taskId: Long, taskMemoryManager: TaskMemoryManager): Unit = {
     if (!taskIdToManager.contains(taskId)) {
       taskIdToManager.put(taskId, taskMemoryManager)
     }
   }

   @volatile def unregisterTask(taskId: Long): Unit = {
     if (taskIdToManager.containsKey(taskId)) {
       taskIdToManager.remove(taskId)
       taskIdToExecMemory.remove(taskId)
       if (taskIdToStoreMemory.contains(taskId)) {
         taskIdToStoreMemory.remove(taskId)
       }

       val execMem = profileOneTaskExecMemory(taskId)
       unreportedTaskIdToExecMemory.put(taskId, execMem)
     }
   }

   def getTaskStoreMemoryUsage(taskId: Long): Long = {
     val storeMem = {
       if (taskIdToStoreMemory.contains(taskId)) {
         taskIdToStoreMemory.get(taskId)
       } else {
         -1L
       }
     }
     storeMem
   }

   def getTaskExecMemoryUsage(taskId: Long): Long = {
     val execMem = {
       if (taskIdToExecMemory.contains(taskId)) {
         taskIdToExecMemory.get(taskId)
       } else if (unreportedTaskIdToExecMemory.contains(taskId)) {
         // if the task is finished but unreported, we delete its storage memory record
         taskIdToStoreMemory.remove(taskId)
         unreportedTaskIdToExecMemory.get(taskId)
       } else {
         -1L
       }
     }
     execMem
   }

   private def profileAllTasksExecMemoryUsage(): Unit = {
     val keyIterator = taskIdToManager.keySet().iterator()
     while (keyIterator.hasNext) {
       val key = keyIterator.next()
       taskIdToExecMemory.put(key, profileOneTaskExecMemory(key))
     }
   }

   private def profileOneTaskExecMemory(taskId: Long): Long = {
     taskIdToManager.get(taskId).getMemoryConsumptionForThisTask
   }

   private[executor] def start(): Unit = {
     val intervalMs = profileInterval

     // Wait a random interval so the heartbeats don't end up in sync
     val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

     val profileTask = new Runnable() {
       override def run(): Unit = Utils.logUncaughtExceptions(profileAllTasksExecMemoryUsage())
     }
     memoryProfileThread.scheduleAtFixedRate(
       profileTask, initialDelay, profileInterval, TimeUnit.MILLISECONDS)
   }

   @volatile def setTaskStoreMemory(taskId: Long, size: Long): Unit = {
     taskIdToStoreMemory.put(taskId, size)
   }
 }
