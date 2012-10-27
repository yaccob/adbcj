package org.adbcj.support;

import org.adbcj.CloseMode;
import org.adbcj.ConnectionManager;
import org.adbcj.DbFuture;

/**
 * Abstract implementation of a {@link ConnectionManager}. It does following things for you:
 *
 *
 * @author roman.stoffel@gamlor.info
 */
public abstract class AbstractConnectionManager implements ConnectionManager {



    public DbFuture<Void> close(){
        return close(CloseMode.CLOSE_GRACEFULLY);
    }
}
