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
    @Override
    public ConnectionManager createConnectionManager(String url, String username,
                                                     String password,
                                                     Map<String, String> properties) throws DbException {
        final String[] firstAndSecondPart = url.split("pooled:");
        if(firstAndSecondPart.length!=2){
            throw new IllegalArgumentException("Expect a URL in the form of adbcj:pooled:[driver]:[database-url]. Got: "+url);
        }
        String nativeUrl = firstAndSecondPart[0] + firstAndSecondPart[1];

        return ConnectionManagerProvider.createConnectionManager(nativeUrl, username, password,properties);
    }

    @Override
    public boolean canHandle(String protocol) {
        return PROTOCOL.equals(protocol.toLowerCase());
    }
}
