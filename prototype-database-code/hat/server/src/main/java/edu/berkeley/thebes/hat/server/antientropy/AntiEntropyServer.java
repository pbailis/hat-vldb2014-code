package edu.berkeley.thebes.hat.server.antientropy;

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ThriftServer;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;


public class AntiEntropyServer implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServer.class);

    AntiEntropyServiceRouter router;
    private AntiEntropyServiceHandler serviceHandler;

    public AntiEntropyServer(AntiEntropyServiceHandler serviceHandler) throws TTransportException {
        router = new AntiEntropyServiceRouter();
        this.serviceHandler = serviceHandler;
    }

    public void run() {
        logger.debug("Starting the anti-entropy server on IP..."+Config.getAntiEntropyServerBindIP());
        ThriftServer.startInCurrentThread(
                new AntiEntropyService.Processor<AntiEntropyServiceHandler>(serviceHandler),
                Config.getAntiEntropyServerBindIP());
    }
}