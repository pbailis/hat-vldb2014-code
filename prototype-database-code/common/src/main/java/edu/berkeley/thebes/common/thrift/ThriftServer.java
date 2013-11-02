package edu.berkeley.thebes.common.thrift;

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.net.InetSocketAddress;

public class ThriftServer {
    /** Starts a new Thrift server on the given bind address with a processor. */
    public static void startInCurrentThread(TProcessor processor, InetSocketAddress bindAddress) {
        try {
            TServerTransport serverTransport = new TServerSocket(bindAddress);
            TServer server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport).processor(processor));
            server.serve();
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    }
}
