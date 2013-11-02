package edu.berkeley.thebes.common.persistence.memory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.persistence.disk.WALBackedPersistenceEngine;

public class MemoryPersistenceEngine implements IPersistenceEngine {
    private final Timer forcePutsTimer = Metrics.newTimer(MemoryPersistenceEngine.class, "force-put-latencies");
    private final Timer putsTimer = Metrics.newTimer(MemoryPersistenceEngine.class, "put-latencies");
    private final Timer getsTimer = Metrics.newTimer(MemoryPersistenceEngine.class, "get-latencies");
    private final Timer deletesTimer = Metrics.newTimer(MemoryPersistenceEngine.class, "delete-latencies");
    
    private Map<String, DataItem> map;
    private DataItem nullItem = new DataItem(ByteBuffer.allocate(0), Version.NULL_VERSION);

    public void open() {
        map = Maps.newConcurrentMap();
        Metrics.newGauge(MemoryPersistenceEngine.class, "num-keys", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return map.size();
            }
        });
    }
    
    public void force_put(String key, DataItem value) {
        TimerContext context = forcePutsTimer.time();
        try {
            map.put(key, value);
        } finally {
            context.stop();
        }
    }

    /**
     * Puts the given value for our key.
     * Does not update the value if the key already exists with a later timestamp.
     */
    @Override
    public void put_if_newer(String key, DataItem value) {
        TimerContext context = putsTimer.time();
        try {
            // TODO: Some form of synchronization is necessary
    //        synchronized (map) {
                // If we already have this key, ensure new item is a more recent version
                if (map.containsKey(key)) {
                    DataItem curItem = map.get(key);
                    if (curItem.getVersion().compareTo(value.getVersion()) > 0) {
                        return;
                    }
                }
    
                // New key or newer timestamp.
                map.put(key, value);
    //        }
        } finally {
            context.stop();
        }
    }

    public DataItem get(String key) {
        TimerContext context = getsTimer.time();
        try {
            DataItem ret = map.get(key);
            if(ret == null)
                ret = nullItem;
    
            return ret;
        } finally {
            context.stop();
        }
    }

    public void delete(String key) {
        TimerContext context = deletesTimer.time();
        try {
            map.remove(key);
        } finally {
            context.stop();
        }
    }

    public void close() {
        return;
    }
}