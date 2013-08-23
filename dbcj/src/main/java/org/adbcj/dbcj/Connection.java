package org.adbcj.dbcj;

import org.adbcj.DbFuture;
import org.adbcj.PreparedQuery;
import org.adbcj.PreparedUpdate;

/**
 * @author foooling@gmail.com
 */
public interface Connection extends java.sql.Connection,ConnectionProperties{
    public DbFuture<PreparedQuery> prepareQuery(String sql);
    public DbFuture<PreparedUpdate> prepareUpdate(String sql);


}
