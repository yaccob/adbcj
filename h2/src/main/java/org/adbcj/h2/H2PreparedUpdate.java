package org.adbcj.h2;

import org.adbcj.DbSessionFuture;
import org.adbcj.PreparedUpdate;
import org.adbcj.Result;
import org.adbcj.support.DefaultDbSessionFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2PreparedUpdate extends AbstractStatement implements PreparedUpdate {
    public H2PreparedUpdate(H2Connection connection, int sessionId, int paramsCount) {
        super(connection, sessionId, paramsCount);
    }

    @Override
    public DbSessionFuture<Result> execute(Object... params) {
        if(paramsCount!=params.length){
            throw new IllegalArgumentException("Expect "+paramsCount+" parameters, but got: "+params.length);
        }
        final Request request = connection.requestCreator().executeUpdateStatement(sessionId, params);
        connection.queResponseHandlerAndSendMessage(request);
        return (DbSessionFuture<Result>) request.getToComplete();
    }
}
