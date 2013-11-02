package edu.berkeley.thebes.common.config;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.Lists;

import edu.berkeley.thebes.common.config.ConfigParameterTypes.AtomicityLevel;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.IsolationLevel;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.PersistenceEngine;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.RoutingMode;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.SessionLevel;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.TransactionMode;

/**
 * These define parameters that can have default values or be set by
 * configuration or command-line interfaces.
 */
public enum ConfigParameters {
    
    CONFIG_FILE(String.class, "conf/thebes.yaml"),
    CLUSTERID(Integer.class, RequirementLevel.COMMON),
    /** 0 to N-1, exactly N servers per cluster */
    SERVERID(Integer.class, RequirementLevel.SERVER_COMMON),
    CLIENTID(Short.class, RequirementLevel.CLIENT_COMMON),
    /** Map of clusters, indexed starting at 1. */
    CLUSTER_CONFIG(Map.class, RequirementLevel.HAT_COMMON),
    GRAPHITE_IP(String.class, ""),
    PERSISTENCE_ENGINE(PersistenceEngine.class, PersistenceEngine.MEMORY),
    PENDING_WRITES_DB(String.class, "pending.db"),
    SOCKET_TIMEOUT(Integer.class, 4000),
    SERVER_PORT(Integer.class, 8080),
    ANTI_ENTROPY_PORT(Integer.class, 8081),
    ANTI_ENTROPY_THREADS(Integer.class, 10),
    TA_ANTI_ENTROPY_THREADS(Integer.class, 2),
    TA_BATCH_TIME(Integer.class, 10000),
    STANDALONE(Boolean.class, false),
    ROUTING_MODE(RoutingMode.class, RoutingMode.NEAREST),
    QUORUM_THREADS(Integer.class, 10),
    TXN_MODE(TransactionMode.class, RequirementLevel.CLIAPP),
    HAT_ISOLATION_LEVEL(IsolationLevel.class, IsolationLevel.NO_ISOLATION),
    ANTIENTROPY_BOOTSTRAP_TIME(Integer.class, 30*1000),
    TWOPL_PORT(Integer.class, 8082),
    TWOPL_REPLICATE_TO_SLAVES(Boolean.class, true),
    TWOPL_TM_PORT(Integer.class, 8083),
    TWOPL_TM_CONFIG(Map.class, RequirementLevel.TWOPL_TM),
    TWOPL_CLUSTER_CONFIG(Map.class, RequirementLevel.TWOPL_COMMON),
    TWOPL_USE_TM(Boolean.class, true),
    SESSION_LEVEL(SessionLevel.class, SessionLevel.NO_SESSION),
    ATOMICITY_LEVEL(AtomicityLevel.class, AtomicityLevel.NO_ATOMICITY),
    LOGGER_LEVEL(String.class, "WARN"),
    DISK_DATABASE_FILE(String.class, "/tmp/thebes.db"),
    DO_CLEAN_DATABASE_FILE(Boolean.class, true),
    DATABASE_CACHE_SIZE(Integer.class, -1),
    METRICS_TO_CONSOLE(Boolean.class, false),
    STORE_PENDING_IN_MEMORY(Boolean.class, false);
    
    /** Note that defaultValue and reqLevels are mutually exclusive. */
    private Class<?> type;
    private Object defaultValue;
    private RequirementLevel[] reqLevels;
    
    private <T> ConfigParameters(Class<T> type, Object defaultValue) {
        this.type = type;
        this.defaultValue = defaultValue;
        this.reqLevels = new RequirementLevel[]{};
    }

    private <T> ConfigParameters(Class<T> type, RequirementLevel reqLevel) {
        this.type = type;
        this.defaultValue = null;
        this.reqLevels = new RequirementLevel[]{ reqLevel };
    }
    
    private <T> ConfigParameters(Class<T> type, RequirementLevel ... reqLevels) {
        this.type = type;
        this.defaultValue = null;
        this.reqLevels = reqLevels;
    }
    
    public String getTextName() {
        return this.name().toLowerCase();
    }
    
    public Object getDefaultValue() {
        return defaultValue;
    }
    
    public boolean requiresLevel(RequirementLevel reqLevel) {
        for (RequirementLevel level : reqLevels) {
            if (level == reqLevel) {
                return true;
            }
        }
        return false;
    }

    public Object castValue(Object o) {
        // If our type is already a superclass of o, we can just return!
        if (type.isAssignableFrom(o.getClass())) {
            return o;
            
        // Casting to custom enumeration types. See {@link ConfigParameterTypes}.
        } else if (Arrays.asList(type.getInterfaces()).contains(ConfigParameterTypes.class)) {
            assert o instanceof String;
            try {
                Method m = type.getMethod("valueOf", String.class);
                String key = o.toString().toUpperCase();
                return m.invoke(null, key);
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot cast " + o + " to " + type);
            }
        
        // Boolean casting
        } else if (type.equals(Boolean.class)) {
            if (Lists.newArrayList("", "true", "on").contains(o)) {
                return Boolean.TRUE;
            } else if (Lists.newArrayList("false", "off").contains(o)) {
                return Boolean.FALSE;
            }
        
        // String->Integer casting
        } else if (type.equals(Integer.class) && o instanceof String) {
            return Integer.parseInt(o.toString());
        }
        else if (type.equals(Short.class) && o instanceof String) {
            return Short.parseShort(o.toString());
        }
        
        throw new IllegalArgumentException("Cannot convert " + o.getClass() + " to " + type);
    }
}