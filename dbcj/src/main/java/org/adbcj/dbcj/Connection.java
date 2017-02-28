package org.adbcj.dbcj;

/**
 * @author foooling@gmail.com
 */
public interface Connection extends java.sql.Connection,ConnectionProperties{
    org.adbcj.Connection getRealConnection();


}
