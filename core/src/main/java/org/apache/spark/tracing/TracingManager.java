package org.apache.spark.tracing;

import org.apache.spark.memory.TaskMemoryManager;
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

    boolean isTracingEnabled;
    /** the IP address of the tracing server */
    private String serverURL;

    /** the port of the tracing server */
    private int serverPort;

    public TracingManager(SparkConf conf) {
        this.conf = conf;
        serverURL = conf.get("spark.tracing.address", "localhost");
        serverPort = conf.getInt("spark.tracing.port", 8089);

        isTracingEnabled = conf.getBoolean("spark.tracing.enabled", false);
    }

    /** establish connection */

    /** transfer a new job to the server */
    public void createJob(JobInfo jobInfo) {
        if (!isTracingEnabled) {
            return;
        }
        TTransport transport = null;
        TProtocol protocol;

        try {
            transport = new TSocket(serverURL, serverPort);
            protocol = new TBinaryProtocol(transport);
            TracingService.Client tClient = new TracingService.Client(protocol);
            transport.open();
            tClient.createJob(jobInfo);

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
        if (!isTracingEnabled) {
            return;
        }
        TTransport transport = null;
        TProtocol protocol;
        try {
            transport = new TSocket(serverURL, serverPort);
            protocol = new TBinaryProtocol(transport);
            TracingService.Client tClient = new TracingService.Client(protocol);
            transport.open();
            tClient.updateJobInfo(jobInfo);

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
    public void createOrUpdateStage(StageInfo stage) {
        if (!isTracingEnabled) {
            return;
        }
        TTransport transport = null;
        TProtocol protocol;
        try {
            transport = new TSocket(serverURL, serverPort);
            protocol = new TBinaryProtocol(transport);
            TracingService.Client tClient = new TracingService.Client(protocol);
            transport.open();
            tClient.createStage(stage);

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
        if (!isTracingEnabled) {
            return;
        }
        TTransport transport = null;
        TProtocol protocol;
        try {
            transport = new TSocket(serverURL, serverPort);
            protocol = new TBinaryProtocol(transport);
            TracingService.Client tClient = new TracingService.Client(protocol);
            transport.open();
            tClient.updateStageInfo(stageInfo);

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
    public void createOrUpdateTaskInfo(TaskInfo taskInfo) {
        if (!isTracingEnabled) {
            return;
        }
        TTransport transport = null;
        TProtocol protocol;
        try {
            transport = new TSocket(serverURL, serverPort);
            protocol = new TBinaryProtocol(transport);
            TracingService.Client tClient = new TracingService.Client(protocol);
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
