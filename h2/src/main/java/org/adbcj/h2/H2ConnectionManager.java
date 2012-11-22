package org.adbcj.h2;

import org.adbcj.CloseMode;
import org.adbcj.Connection;
import org.adbcj.DbException;
import org.adbcj.DbFuture;
import org.adbcj.support.AbstractConnectionManager;
import org.adbcj.support.LoginCredentials;

import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2ConnectionManager extends AbstractConnectionManager {

    public H2ConnectionManager(String host, int port, LoginCredentials credentials, Map<String, String> properties) {
        super(properties);
    }

    @Override
    public DbFuture<Connection> connect() {
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
