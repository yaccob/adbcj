package org.adbcj.h2;

import org.adbcj.*;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2PreparedQuery implements PreparedQuery {
    public H2PreparedQuery(H2Connection connection, int sessionId, int paramsCount) {
        //To change body of created methods use File | Settings | File Templates.
    }

    @Override
    public DbSessionFuture<ResultSet> execute(Object... params) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public <T> DbSessionFuture<T> executeWithCallback(ResultHandler<T> eventHandler, T accumulator, Object... params) {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public boolean isClosed() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public DbFuture<Void> close() {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}
