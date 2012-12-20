package org.adbcj.mysql.codec.decoding;

import org.adbcj.Result;
import org.adbcj.mysql.codec.MySqlConnection;
import org.adbcj.mysql.codec.MysqlResult;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.adbcj.support.DefaultDbSessionFuture;

import java.util.ArrayList;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ExpectUpdateResult<T> extends ExpectOK {

    public ExpectUpdateResult(DefaultDbSessionFuture<Result> future) {
        super(future,(MySqlConnection) future.getSession());
        //To change body of created methods use File | Settings | File Templates.
    }

    @Override
    protected ResultAndState handleOk(OkResponse.RegularOK regularOK) {
        return handleUpdateResult(regularOK,(DefaultDbSessionFuture<Result>) futureToComplete);
    }

    static ResultAndState handleUpdateResult(OkResponse.RegularOK regularOK, DefaultDbSessionFuture<Result> futureToComplete) {
        ArrayList<String> warnings = new ArrayList<String>(regularOK.getWarningCount());
        for (int i = 0; i < regularOK.getWarningCount(); i++) {
            warnings.add(regularOK.getMessage());
        }
        MysqlResult result = new MysqlResult(regularOK.getAffectedRows(),warnings,regularOK.getInsertId());
        futureToComplete.trySetResult(result);
        return new ResultAndState(new AcceptNextResponse((MySqlConnection) futureToComplete.getSession()),regularOK );
    }
}
