package edu.berkeley.thebes.client;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.common.log4j.Log4JConfig;
import edu.berkeley.thebes.hat.client.ThebesHATClient;
import edu.berkeley.thebes.hat.client.clustering.QuorumReplicaRouter;
import edu.berkeley.thebes.twopl.client.ThebesTwoPLClient;
import edu.berkeley.thebes.twopl.common.ThebesTwoPLTransactionClient;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

// Wrapper class for various thebes clients

public class ThebesClient implements IThebesClient {
    private IThebesClient internalClient;

    public ThebesClient() {
    }

    @Override
    public void open() throws TTransportException, ConfigurationException, IOException {
        Config.initializeClient();
        Log4JConfig.configureLog4J();

        switch (Config.getThebesTxnMode()) {
        case HAT:
            internalClient = new ThebesHATClient();
            break;
        case TWOPL:
            if (Config.shouldUseTwoPLTM()) {
                internalClient = new ThebesTwoPLClient();
            } else {
                internalClient = new ThebesTwoPLTransactionClient();
            }
            break;
        default:
            throw new ConfigurationException("Unrecognized txn mode: " + Config.getThebesTxnMode());
        }

        internalClient.open();
    }

    @Override
    public void beginTransaction() throws TException {
        internalClient.beginTransaction();
    }

    @Override
    public boolean commitTransaction() throws TException {
        return internalClient.commitTransaction();
    }

    @Override
    public void abortTransaction() throws TException {
        internalClient.abortTransaction();
    }

    @Override
    public boolean put(String key, ByteBuffer value) throws TException {
        return internalClient.put(key, value);
    }
    
    public boolean unsafe_load(String key, ByteBuffer value) throws TException {
        if (internalClient instanceof ThebesTwoPLTransactionClient) {
            return ((ThebesTwoPLTransactionClient) internalClient).unsafe_load(key, value);
        } else {
            // Client does not support special behavior
            return internalClient.put(key, value);
        }
    }

    @Override
    public ByteBuffer get(String key) throws TException {
        return internalClient.get(key);
    }

    @Override
    public void sendCommand(String cmd) throws TException {
        internalClient.sendCommand(cmd);
    }

    public void close() {
        internalClient.close();
    }
}