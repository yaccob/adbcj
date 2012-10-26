package org.adbcj.connectionpool;

import org.adbcj.*;

import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public class MockConnectionFactory implements ConnectionManagerFactory {
    @Override
    public ConnectionManager createConnectionManager(String url,
                                                     String username,
                                                     String password,
                                                     Map<String, String> properties) throws DbException {
        return new MockConnectionManager();
    }

    @Override
    public boolean canHandle(String protocol) {
        return "mock".equals(protocol);
    }

    private static class MockConnectionManager implements ConnectionManager {
        @Override
        public DbFuture<Connection> connect() {
            throw new Error("Not implemented yet: TODO");  //TODO: Implement
        }

        @Override
        public DbFuture<Void> close() throws DbException {
            throw new Error("Not implemented yet: TODO");  //TODO: Implement
        }

        @Override
        public DbFuture<Void> close(CloseMode mode) throws DbException {
            throw new Error("Not implemented yet: TODO");  //TODO: Implement
        }

        @Override
        public boolean isClosed() {
            throw new Error("Not implemented yet: TODO");  //TODO: Implement
        }


    }
}
