package org.adbcj.connectionpool;

import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class ConfigInfo {
    public static final long MAX_DEFAULT_CONNECTIONS = 50;
    public static final long MAX_DEFAULT_WAIT_FOR_CONNECTIONS = 500;
    public static final String POOL_MAX_CONNECTIONS = "pool.maxConnections";
    public static final String MAX_WAIT_FOR_CONNECTIONS = "pool.maxWaitForConnection";
    private final long maxConnections;
    private final long maxWaitForConnectionsInMillisec;

    ConfigInfo(Map<String, String> properties) {
        this.maxConnections = longPositiveProperty(properties,
                POOL_MAX_CONNECTIONS,
                MAX_DEFAULT_CONNECTIONS);
        this.maxWaitForConnectionsInMillisec = longPositiveProperty(properties,
                MAX_WAIT_FOR_CONNECTIONS,
                MAX_DEFAULT_WAIT_FOR_CONNECTIONS);
    }


    public long getMaxConnections() {
        return maxConnections;
    }

    public long getMaxWaitForConnectionsInMillisec() {
        return maxWaitForConnectionsInMillisec;
    }
    private long longPositiveProperty(Map<String, String> properties, String property, long defaultValue) {
        return mustBePositive(longProperty(properties, property, defaultValue),property);
    }

    private long mustBePositive(long value, String message) {
        if(value<=0){
            throw new IllegalArgumentException("The "+message+" has to be positive");
        }
        return value;
    }

    static long longProperty(Map<String, String> properties, String property, long defaultValue) {
        final String maxConnections = properties.get(property);
        try{
            return Integer.parseInt(maxConnections);
        }catch (NumberFormatException e){
            return defaultValue;
        }
    }
}
