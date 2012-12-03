package org.adbcj.h2;

import org.adbcj.*;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.DefaultDbSessionFuture;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2PreparedQuery implements PreparedQuery {
    private final H2Connection connection;
    private final int sessionId;
    private final int paramsCount;
    private volatile DefaultDbFuture<Void> closeFuture =null;

    public H2PreparedQuery(H2Connection connection, int sessionId, int paramsCount) {
        this.connection = connection;
        this.sessionId = sessionId;
        this.paramsCount = paramsCount;
    }

    @Override
    public DbSessionFuture<ResultSet> execute(Object... params) {
        if(paramsCount!=params.length){
            throw new IllegalArgumentException("Expect "+paramsCount+" parameters, but got: "+params.length);
        }
        DefaultResultEventsHandler eventHandler = new DefaultResultEventsHandler();
        DefaultResultSet resultSet = new DefaultResultSet();
        return executeWithCallback((ResultHandler) eventHandler,resultSet,params);
    }

    @Override
    public <T> DbSessionFuture<T> executeWithCallback(ResultHandler<T> eventHandler, T accumulator, Object... params) {
        DefaultDbSessionFuture<T> resultFuture = new DefaultDbSessionFuture<T>(connection);
        int queryId = connection.nextId();
        final Request request = Request.executeQueryStatement(eventHandler,
                accumulator,
                resultFuture,
                sessionId,
                queryId,
                params);
        connection.queResponseHandlerAndSendMessage(request);
        return resultFuture;
    }

    @Override
    public boolean isClosed() {
        return closeFuture!=null;
    }

    @Override
    public DbFuture<Void> close() {
        synchronized (connection.connectionLock()){
            if(null!=closeFuture){
                return closeFuture;
            } else{
                this.closeFuture = new DefaultDbFuture<Void>();
                final Request request = Request.executeCloseStatement(connection,
                        closeFuture,
                        sessionId);
                connection.queResponseHandlerAndSendMessage(request);
                return closeFuture;
            }
        }
    }
}
