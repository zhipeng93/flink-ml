package org.apache.flink.ml.common.broadcast;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** BroadcastContext stores the broadcast variables in static variables. */
public class BroadcastContext {

    /** Store broadcast DataStreams in a map. The key is broadcastName. */
    private static Map<String, List<?>> broadcastVariables = new HashMap<>();
    /** Store reference count of DataStreams in a map. */
    private static Map<String, Integer> broadcastVariableRefCnt = new HashMap<>();
    /** whether one broadcast variable has been finished reading. */
    private static Map<String, Boolean> broadcastVariableCacheFinshed = new HashMap<>();
    /** the lock for concurrent access. */
    private static ReadWriteLock rwlock = new ReentrantReadWriteLock();

    /**
     * if there are multiple slots in a TM, only one task needs to store the cache list.
     *
     * @param name
     * @param elements
     * @return
     */
    public static boolean tryPutCacheList(String name, List<?> elements) {
        boolean success;
        rwlock.writeLock().lock();
        try {
            if (broadcastVariables.containsKey(name)) {
                broadcastVariableRefCnt.put(name, broadcastVariableRefCnt.get(name) + 1);
                success = false;
            } else {
                broadcastVariables.put(name, elements);
                broadcastVariableRefCnt.put(name, 1);
                success = true;
            }
        } finally {
            rwlock.writeLock().unlock();
        }
        return success;
    }

    /**
     * check whether all elements of DataStream_name has been stored in context.
     *
     * @param name
     * @return
     */
    public static boolean isBroadcastVariableReady(String name) {
        boolean success = false;
        rwlock.readLock().lock();
        try {
            if (broadcastVariableCacheFinshed.containsKey(name)
                    && broadcastVariableCacheFinshed.get(name)) {
                success = true;
            }
        } finally {
            rwlock.readLock().unlock();
        }
        return success;
    }

    /**
     * mark all elements of DataStream_name has been stored in context.
     *
     * @param name
     */
    public static void setBroadcastVariableReady(String name) {
        rwlock.writeLock().lock();
        try {
            broadcastVariableCacheFinshed.put(name, true);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    /**
     * clear the broadcast variable.
     *
     * @param name
     */
    public static void tryClearBroadcastVariable(String name) {
        rwlock.writeLock().lock();
        try {
            int refCnt = broadcastVariableRefCnt.get(name);
            if (refCnt > 1) {
                broadcastVariableRefCnt.put(name, refCnt - 1);
            } else {
                broadcastVariables.remove(name);
                broadcastVariableCacheFinshed.remove(name);
            }
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    public static <T> List<T> getBroadcastVariable(String name) {
        List<?> result = null;
        rwlock.readLock().lock();
        try {
            result = broadcastVariables.get(name);
        } finally {
            rwlock.readLock().unlock();
        }
        return (List<T>) result;
    }
}
