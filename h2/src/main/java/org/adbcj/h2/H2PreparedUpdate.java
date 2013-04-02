package org.adbcj.h2;

import org.adbcj.DbFuture;
import org.adbcj.PreparedUpdate;
import org.adbcj.Result;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2PreparedUpdate extends AbstractStatement implements PreparedUpdate {
    public H2PreparedUpdate(H2Connection connection, int sessionId, int paramsCount) {
        super(connection, sessionId, paramsCount);
    }

    @Override
    public DbFuture<Result> execute(Object... params) {
        connection.checkClosed();
        if(paramsCount!=params.length){
            throw new IllegalArgumentException("Expect "+paramsCount+" parameters, but got: "+params.length);
        }
        final Request request = connection.requestCreator().executeUpdateStatement(sessionId, params);
        connection.queRequest(request);
        return (DbFuture<Result>) request.getToComplete();
    }
}
