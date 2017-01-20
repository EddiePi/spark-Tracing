package org.apache.spark.tracing;

import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.scheduler.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import org.apache.spark.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Created by Eddie on 2017/1/18.
 */
public class TracingManager {

    private static final Logger logger = LoggerFactory.getLogger(TaskMemoryManager.class);

    private SparkConf conf;

    boolean isTracingEnalbed = conf.getBoolean("spark.tracing.enabled", false);
    /** the IP address of the tracing server */
    private String serverURL;

    /** the port of the tracing server */
    private int serverPort;

    private TTransport transport;
    private TProtocol protocol;

    public TracingManager(SparkConf conf) {
        this.conf = conf;
        serverURL = conf.get("spark.tracing.address", "localhost");
        serverPort = conf.getInt("spark.tracing.port", 8089);
        transport = new TSocket(serverURL, serverPort);
        protocol = new TBinaryProtocol(transport);
    }

    /** transfer a new job to the server */
    public void createJob(JobInfo jobInfo) {
        if (!isTracingEnalbed) {
            return;
        }
        try {
            JobManagementService.Client jClient = new JobManagementService.Client(protocol);
            transport.open();
            jClient.createJob(jobInfo);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
    }

    public void updateJobInfo(JobInfo jobInfo) {
        if (!isTracingEnalbed) {
            return;
        }
        try {
            JobManagementService.Client jClient = new JobManagementService.Client(protocol);
            transport.open();
            jClient.updateJobInfo(jobInfo);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
    }

    /** transfer new stages to the server */
    public void createStageList(StageList stageList) {
        if (!isTracingEnalbed) {
            return;
        }
        try {
            StageManagementService.Client sClient = new StageManagementService.Client(protocol);
            transport.open();
            sClient.createStageList(stageList);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
    }

    /** update the status of a stage */
    public void updateStageInfo(StageInfo stageInfo) {
        if (!isTracingEnalbed) {
            return;
        }
        try {
            StageManagementService.Client sClient = new StageManagementService.Client(protocol);
            transport.open();
            sClient.updateStageInfo(stageInfo);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
    }

    /** transfer a new task set to the server */
    public void createNewTaskSet(TaskSetInfo taskSetInfo) {
        if (!isTracingEnalbed) {
            return;
        }
        try {
            TaskManagementService.Client tClient = new TaskManagementService.Client(protocol);
            transport.open();
            tClient.createTaskSet(taskSetInfo);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
    }

    /** update the status of a task */
    public void updateTaskInfo(TaskInfo taskInfo) {
        if (!isTracingEnalbed) {
            return;
        }
        try {
            TaskManagementService.Client tClient = new TaskManagementService.Client(protocol);
            transport.open();
            tClient.updateTaskInfo(taskInfo);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
    }

}
