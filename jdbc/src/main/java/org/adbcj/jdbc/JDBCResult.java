package org.adbcj.jdbc;

import org.adbcj.DbException;
import org.adbcj.ResultSet;
import org.adbcj.support.DefaultResult;
import org.adbcj.support.DefaultResultEventsHandler;
import org.adbcj.support.DefaultResultSet;

import java.sql.SQLException;
import java.util.List;

/**
 * @author roman.stoffel@gamlor.info
 * @since 10.05.12
 */
class JDBCResult extends DefaultResult {
    private final ResultSet generatedKeys;

    public JDBCResult(long affectedRows,
                      List<String> warnings,
                      java.sql.ResultSet generatedKeys) {
        super(affectedRows, warnings);
        DefaultResultSet resultSet = new DefaultResultSet();
        try {
            ResultSetCopier.fillResultSet(generatedKeys,new DefaultResultEventsHandler(),resultSet);
            this.generatedKeys = resultSet;
        } catch (SQLException e) {
            throw new DbException(e);
        }
    }

    @Override
    public ResultSet getGeneratedKeys() {
        return generatedKeys;
    }
}
