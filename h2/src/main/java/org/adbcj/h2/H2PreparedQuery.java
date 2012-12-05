package org.adbcj.h2;

import org.adbcj.DbSessionFuture;
import org.adbcj.PreparedQuery;
import org.adbcj.ResultHandler;
import org.adbcj.ResultSet;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2PreparedQuery extends AbstractStatement implements PreparedQuery {

    public H2PreparedQuery(H2Connection connection, int sessionId, int paramsCount) {
        super(connection, sessionId, paramsCount);
    }

    @Override
    public DbSessionFuture<ResultSet> execute(Object... params) {
        DefaultResultEventsHandler eventHandler = new DefaultResultEventsHandler();
        DefaultResultSet resultSet = new DefaultResultSet();
        return executeWithCallback((ResultHandler) eventHandler,resultSet,params);
    }

    @Override
    public <T> DbSessionFuture<T> executeWithCallback(ResultHandler<T> eventHandler, T accumulator, Object... params) {
        if(paramsCount!=params.length){
            throw new IllegalArgumentException("Expect "+paramsCount+" parameters, but got: "+params.length);
        }
        final Request request = connection.requestCreator().executeQueryStatement(eventHandler,
                accumulator,
                sessionId,
                params);
        connection.queResponseHandlerAndSendMessage(request);
        return (DbSessionFuture<T>) request.getToComplete();
    }

}
