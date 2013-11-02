package edu.berkeley.thebes.common.thrift;

import com.google.common.base.Objects;

/** Simply stores all the attributes of some external server. */
public class ServerAddress {
    private final int clusterID;
    private final int serverID;
    private final String ip;
    private int port;
    
    public ServerAddress(int clusterID, int serverID, String ip, int port) {
        this.clusterID = clusterID;
        this.serverID = serverID;
        this.ip = ip;
        this.port = port;
    }
    
    public int getClusterID() {
        return clusterID;
    }
    public int getServerID() {
        return serverID;
    }
    public String getIP() {
        return ip;
    }
    public int getPort() {
        return port;
    }
    
    @Override
    public String toString() {
        return ip + ":" + port + " [" + clusterID + ", " + serverID + "]";
    }
    
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ServerAddress)) {
            return false;
        }
        
        ServerAddress other = (ServerAddress) o;
        return Objects.equal(clusterID, other.getClusterID())
                && Objects.equal(serverID, other.getServerID())
                && Objects.equal(ip, other.getIP())
                && Objects.equal(port, other.getPort());
    }
    
    public int hashCode() {
        return Objects.hashCode(clusterID, serverID, ip, port);
    }
}
