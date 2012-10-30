package org.adbcj.connectionpool;

import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerFactory;
import org.adbcj.ConnectionManagerProvider;
import org.adbcj.DbException;

import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public class PooledConnectionManagerFactory implements ConnectionManagerFactory {
    private static final String PROTOCOL = "pooled";
    public static final String POOL_MAX_CONNECTIONS = "pool.maxConnections";

    @Override
    public ConnectionManager createConnectionManager(String url, String username,
                                                     String password,
                                                     Map<String, String> properties) throws DbException {
        final String[] firstAndSecondPart = url.split("pooled:");
        if(firstAndSecondPart.length!=2){
            throw new IllegalArgumentException("Expect a URL in the form of adbcj:pooled:[driver]:[database-url]. Got: "+url);
        }
        String nativeUrl = firstAndSecondPart[0] + firstAndSecondPart[1];

        return new PooledConnectionManager(
                ConnectionManagerProvider.createConnectionManager(nativeUrl, username, password,properties),
                new ConfigInfo(maxConnections(properties)));
    }

    private int maxConnections(Map<String, String> properties) {
        final String maxConnections = properties.get(POOL_MAX_CONNECTIONS);
        try{
            return Integer.parseInt(maxConnections);
        }catch (NumberFormatException e){
            return ConfigInfo.MAX_DEFAULT_CONNECTIONS;
        }
    }

    @Override
    public boolean canHandle(String protocol) {
        return PROTOCOL.equals(protocol.toLowerCase());
    }
}
