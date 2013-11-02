package edu.berkeley.thebes.common.interfaces;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.naming.ConfigurationException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface IThebesClient {
    public void open() throws TTransportException, ConfigurationException, IOException;

    public void beginTransaction() throws TException;

    public void abortTransaction() throws TException;
    public boolean commitTransaction() throws TException;

    public boolean put(String key, ByteBuffer value) throws TException;
    public ByteBuffer get(String key) throws TException;
    /** Sends an arbitrary, parseable command. */  
    public void sendCommand(String cmd) throws TException;

    public void close();
}