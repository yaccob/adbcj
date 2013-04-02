package org.adbcj.mysql.codec.decoding;

import org.adbcj.Result;
import org.adbcj.mysql.codec.MySqlConnection;
import org.adbcj.mysql.codec.MysqlResult;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.OneArgFunction;

import java.util.ArrayList;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ExpectUpdateResult<T> extends ExpectOK {

    private final OneArgFunction<MysqlResult, T> transformation;

    public ExpectUpdateResult(DefaultDbFuture<Result> future,
                              MySqlConnection connection) {
        this(future,connection,OneArgFunction.ID_FUNCTION);
    }
    public ExpectUpdateResult(DefaultDbFuture<Result> future,
                              MySqlConnection connection,
                              OneArgFunction<MysqlResult,T> transformation) {
        super(future,connection);
        this.transformation = transformation;
    }

    @Override
    protected ResultAndState handleOk(OkResponse.RegularOK regularOK) {
        return handleUpdateResult(regularOK,
                (DefaultDbFuture<T>) futureToComplete,
                connection,
                transformation);
    }

    static <TFutureType> ResultAndState handleUpdateResult(OkResponse.RegularOK regularOK,
                                                 DefaultDbFuture<TFutureType> futureToComplete,
                                                 MySqlConnection connection,
                                                 OneArgFunction<MysqlResult,TFutureType> transformation) {
        ArrayList<String> warnings = new ArrayList<String>(regularOK.getWarningCount());
        for (int i = 0; i < regularOK.getWarningCount(); i++) {
            warnings.add(regularOK.getMessage());
        }
        MysqlResult result = new MysqlResult(regularOK.getAffectedRows(),warnings,regularOK.getInsertId());
        futureToComplete.trySetResult(transformation.apply(result));
        return new ResultAndState(new AcceptNextResponse(connection),regularOK );
    }
}
