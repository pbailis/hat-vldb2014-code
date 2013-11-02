package edu.berkeley.thebes.hat.common.thrift;

import java.io.IOException;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.berkeley.thebes.common.config.Config;

public class ThriftUtil {
    public static ReplicaService.Client getReplicaServiceSyncClient(
            String host, int port) throws TTransportException {
        TProtocol protocol = createProtocol(host, port, Config.getSocketTimeout());
        return new ReplicaService.Client(protocol);
    }

    public static ReplicaService.AsyncClient getReplicaServiceAsyncClient(
            String host, int port) throws TTransportException, IOException {
        return new ReplicaService.AsyncClient(new TBinaryProtocol.Factory(),
                                              new TAsyncClientManager(),
                                              new TNonblockingSocket(host, port, Config.getSocketTimeout()));
    }

    public static AntiEntropyService.Client getAntiEntropyServiceClient(
            String host, int port) throws TTransportException {
        TProtocol protocol = createProtocol(host, port, Config.getSocketTimeout());
        return new AntiEntropyService.Client(protocol);
    }

    public static AntiEntropyService.AsyncClient getAntiEntropyServiceAsyncClient(
            String host, int port) throws TTransportException, IOException {
        return new AntiEntropyService.AsyncClient(new TBinaryProtocol.Factory(), new TAsyncClientManager(),
                                                  new TNonblockingSocket(host, port, Config.getSocketTimeout()));
    }

    private static TProtocol createProtocol(String host, int port, int timeout)
            throws TTransportException {
        TTransport transport = new TSocket(host, port, timeout);
        transport.open();
        return new TBinaryProtocol(transport);
    }
    
    private static TProtocol createFramedProtocol(String host, int port, int timeout)
            throws TTransportException {
        TTransport transport = new TSocket(host, port, timeout);
        transport.open();
        return new TBinaryProtocol(new TFramedTransport(transport));
    }
}