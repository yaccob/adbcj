package org.adbcj.connectionpool;

import org.adbcj.*;

import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public class MockConnectionFactory implements ConnectionManagerFactory {
    private static ThreadLocal<MockConnectionManager> lastInstance = new ThreadLocal<MockConnectionManager>();
    @Override
    public ConnectionManager createConnectionManager(String url,
                                                     String username,
                                                     String password,
                                                     Map<String, String> properties) throws DbException {
        final MockConnectionManager instance = new MockConnectionManager();
        lastInstance.set(instance);
        return instance;
    }

    public static MockConnectionManager lastInstanceRequestedOnThisThread(){
        return lastInstance.get();
    }

    @Override
    public boolean canHandle(String protocol) {
        return "mock".equals(protocol);
    }

}
