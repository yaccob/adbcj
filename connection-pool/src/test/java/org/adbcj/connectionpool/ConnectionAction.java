package org.adbcj.connectionpool;

import org.adbcj.Connection;
import org.adbcj.DbFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public interface ConnectionAction {
    DbFuture invoke(Connection connection);
}
