package edu.berkeley.thebes.twopl.common;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLThriftUtil;

import java.util.ArrayList;
import java.util.List;

/** Helps route traffic to the master of each replica set. */
public class TwoPLMasterRouter {
    /** Contains the ordered list of master replicas, one per set of replicas. */
    private List<TwoPLMasterReplicaService.Client> masterReplicas;

    public TwoPLMasterRouter() throws TTransportException {
        List<ServerAddress> serverIPs = Config.getMasterServers();
        masterReplicas = new ArrayList<TwoPLMasterReplicaService.Client>(serverIPs.size());

        for (ServerAddress server : serverIPs) {
            masterReplicas.add(TwoPLThriftUtil.getMasterReplicaServiceClient(
                    server.getIP(), server.getPort()));
        }
    }

    public TwoPLMasterReplicaService.Client getMasterByKey(String key) throws TTransportException {
        int index = RoutingHash.hashKey(key, masterReplicas.size());
        TwoPLMasterReplicaService.Client client = masterReplicas.get(index);
        TSocket sock = (TSocket) client.getInputProtocol().getTransport();
        if (!sock.isOpen()) {
            // TODO: Logger
            System.err.println("ERROR: Client for key '" + key + "' has closed! Opening new channel.");
            client = TwoPLThriftUtil.getMasterReplicaServiceClient(
                    sock.getSocket().getInetAddress().getHostAddress(),
                    sock.getSocket().getPort());
            masterReplicas.set(index, client);
        }
        return client;
    }
    
    public ServerAddress getMasterAddressByKey(String key) {
        List<ServerAddress> servers = Config.getMasterServers();
        return servers.get(RoutingHash.hashKey(key, servers.size()));
    }

    public void refreshMasterForKey(String key) throws TTransportException {
        int index = RoutingHash.hashKey(key, masterReplicas.size());
        TwoPLMasterReplicaService.Client client = masterReplicas.get(index);
        TSocket sock = (TSocket) client.getInputProtocol().getTransport();
        // TODO: Logger
        System.err.println("WARNING: Client for key '" + key + "' may have closed! Opening new channel.");
        client = TwoPLThriftUtil.getMasterReplicaServiceClient(
                sock.getSocket().getInetAddress().getHostAddress(),
                sock.getSocket().getPort());
        masterReplicas.set(index, client);
    }
}