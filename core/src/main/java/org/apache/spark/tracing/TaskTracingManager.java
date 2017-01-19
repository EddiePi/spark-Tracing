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
public class TaskTracingManager {

    private static final Logger logger = LoggerFactory.getLogger(TaskMemoryManager.class);

    SparkConf conf;

    /** the IP address of the tracing server */
    private String serverURL;

    /** the port of the tracing server */
    private int serverPort;

    public TaskTracingManager(SparkConf conf) {
        this.conf = conf;
        serverURL = conf.get("spark.tracing.address", "localhost");
        serverPort = conf.getInt("spark.tracing.port", 8089);
    }

    public boolean updateTaskInfo(TaskAttempt ta) {
        TTransport transport = null;
        boolean result = false;
        try {
            transport = new TSocket(serverURL, serverPort);
            // 协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(transport);
            // TProtocol protocol = new TCompactProtocol(transport);
            // TProtocol protocol = new TJSONProtocol(transport);
            UpdateTaskInfoService.Client client = new UpdateTaskInfoService.Client(protocol);
            transport.open();
            result = client.updateTaskAttempt(ta);
            logger.debug("update result: " + result);
        } catch (TTransportException e) {
            logger.error("TTransportException", e);
        } catch (TException e) {
            logger.error("TException", e);
        } finally {
            if (null != transport) {
                transport.close();
            }
            return result;
        }
    }
}
