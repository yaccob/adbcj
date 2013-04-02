package org.adbcj.connectionpool;

import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class ConfigInfo {
    public static final long MAX_DEFAULT_CONNECTIONS = 50;
    public static final long MAX_DEFAULT_WAIT_FOR_CONNECTIONS = 500;
    public static final int DEFAULT_STATEMENT_CACHE_SIZE = 64;
    public static final String POOL_MAX_CONNECTIONS = "pool.maxConnections";
    public static final String MAX_WAIT_FOR_CONNECTIONS = "pool.maxWaitForConnection";
    public static final String STATEMENT_CACHE_SIZE = "pool.statementCacheSize";
    private final long maxConnections;
    private final long maxWaitForConnectionsInMillisec;
    private final int statementCacheSize;

    ConfigInfo(Map<String, String> properties) {
        this.maxConnections = longPositiveProperty(properties,
                POOL_MAX_CONNECTIONS,
                MAX_DEFAULT_CONNECTIONS);
        this.maxWaitForConnectionsInMillisec = longPositiveProperty(properties,
                MAX_WAIT_FOR_CONNECTIONS,
                MAX_DEFAULT_WAIT_FOR_CONNECTIONS);
        this.statementCacheSize = intPositiveProperty(properties,
                STATEMENT_CACHE_SIZE,
                DEFAULT_STATEMENT_CACHE_SIZE);
    }


    public long getMaxConnections() {
        return maxConnections;
    }

    public long getMaxWaitForConnectionsInMillisec() {
        return maxWaitForConnectionsInMillisec;
    }

    public int getStatementCacheSize() {
        return statementCacheSize;
    }

    private long longPositiveProperty(Map<String, String> properties, String property, long defaultValue) {
        return mustBePositive(longProperty(properties, property, defaultValue),property);
    }
    private int intPositiveProperty(Map<String, String> properties, String property, int defaultValue) {
        return notNegative(intProperty(properties, property, defaultValue),property);
    }

    private long mustBePositive(long value, String message) {
        if(value<=0){
            throw new IllegalArgumentException("The "+message+" has to be positive, but was: "+value);
        }
        return value;
    }
    private int notNegative(int value, String message) {
        if(value<0){
            throw new IllegalArgumentException("The "+message+" has to be larger than 0, but was: "+value);
        }
        return value;
    }

    static long longProperty(Map<String, String> properties, String property, long defaultValue) {
        final String maxConnections = properties.get(property);
        try{
            return Long.parseLong(maxConnections);
        }catch (NumberFormatException e){
            return defaultValue;
        }
    }
    static int intProperty(Map<String, String> properties, String property, int defaultValue) {
        final String maxConnections = properties.get(property);
        try{
            return Integer.parseInt(maxConnections);
        }catch (NumberFormatException e){
            return defaultValue;
        }
    }
}
