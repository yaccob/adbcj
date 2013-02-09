package org.adbcj.mysql.codec.decoding;

import org.adbcj.Result;
import org.adbcj.mysql.codec.MySqlConnection;
import org.adbcj.mysql.codec.MysqlResult;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.adbcj.support.DefaultDbSessionFuture;
import org.adbcj.support.OneArgFunction;

import java.util.ArrayList;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ExpectUpdateResult<T> extends ExpectOK {

    private final OneArgFunction<MysqlResult, T> transformation;

    public ExpectUpdateResult(DefaultDbSessionFuture<Result> future) {
        this(future,OneArgFunction.ID_FUNCTION);
    }
    public ExpectUpdateResult(DefaultDbSessionFuture<Result> future,
                              OneArgFunction<MysqlResult,T> transformation) {
        super(future,(MySqlConnection) future.getSession());
        this.transformation = transformation;
    }

    @Override
    protected ResultAndState handleOk(OkResponse.RegularOK regularOK) {
        return handleUpdateResult(regularOK,
                (DefaultDbSessionFuture<T>) futureToComplete,
                transformation);
    }

    static <TFutureType> ResultAndState handleUpdateResult(OkResponse.RegularOK regularOK,
                                                 DefaultDbSessionFuture<TFutureType> futureToComplete,
                                                 OneArgFunction<MysqlResult,TFutureType> transformation) {
        ArrayList<String> warnings = new ArrayList<String>(regularOK.getWarningCount());
        for (int i = 0; i < regularOK.getWarningCount(); i++) {
            warnings.add(regularOK.getMessage());
        }
        MysqlResult result = new MysqlResult(regularOK.getAffectedRows(),warnings,regularOK.getInsertId());
        futureToComplete.trySetResult(transformation.apply(result));
        return new ResultAndState(new AcceptNextResponse((MySqlConnection) futureToComplete.getSession()),regularOK );
    }
}
