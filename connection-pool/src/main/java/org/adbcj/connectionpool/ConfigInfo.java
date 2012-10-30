package org.adbcj.connectionpool;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class ConfigInfo {
    public static final int MAX_DEFAULT_CONNECTIONS = 50;
    private final int maxConnections;

    public ConfigInfo(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getMaxConnections() {
        return maxConnections;
    }
}
