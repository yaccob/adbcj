package org.adbcj.mysql.codec.decoding;

import org.adbcj.mysql.codec.*;
import org.adbcj.mysql.codec.packets.EofResponse;
import org.adbcj.mysql.codec.packets.PreparedStatementToBuild;
import org.adbcj.mysql.codec.packets.StatementPreparedEOF;
import org.adbcj.support.DefaultDbSessionFuture;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author roman.stoffel@gamlor.info
 * @since 12.04.12
 */
abstract class FinishPrepareStatement extends DecoderState {

    protected final PreparedStatementToBuild statement;
    protected final DefaultDbSessionFuture<MySqlPreparedStatement> toComplete;

    FinishPrepareStatement(PreparedStatementToBuild statement,DefaultDbSessionFuture<MySqlPreparedStatement> toComplete) {
        this.statement = statement;
        this.toComplete = toComplete;
    }

    protected void readAllAndIgnore(BoundedInputStream in) throws IOException {
        in.read(new byte[in.getRemaining()]);
    }

    public static DecoderState create(PreparedStatementToBuild statement,
                                      DefaultDbSessionFuture<MySqlPreparedStatement> toComplete) {
        if (statement.getParams() > 0) {
            return new ReadParameters(statement.getParams(), statement, toComplete);
        } else if (statement.getColumns() > 0) {
            return new ReadColumns(statement.getColumns(), statement, toComplete);
        } else {
            return new AcceptNextResponse((MySqlConnection) toComplete.getSession());
        }
    }

    private static class ReadParameters extends FinishPrepareStatement {
        private final int parametersToParse;

        public ReadParameters(int parametersToParse,
                              PreparedStatementToBuild statement,
                              DefaultDbSessionFuture<MySqlPreparedStatement> toComplete) {
            super(statement,toComplete);

            this.parametersToParse = parametersToParse;
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, Channel channel) throws IOException {
            int typesCount = statement.getParametersTypes().size();
            MysqlType newType = FieldDecodingState.parseField(in, typesCount).getMysqlType();
            List<MysqlType> types = new ArrayList<MysqlType>(typesCount + 1);
            types.addAll(statement.getParametersTypes());
            types.add(newType);
            PreparedStatementToBuild newStatement = new PreparedStatementToBuild(length, packetNumber,
                    statement.getPreparedStatement(), types);
            int restOfParams = parametersToParse - 1;
            if (restOfParams > 0) {
                return result(new ReadParameters(restOfParams, newStatement,toComplete), statement);
            } else {
                return result(new EofAndColumns(newStatement,toComplete), statement);
            }
        }

        @Override
        public String toString() {
            return "PREPARED-STATEMENT-READ-PARAMETERS";
        }
    }

    private static class EofAndColumns extends FinishPrepareStatement {

        public EofAndColumns(PreparedStatementToBuild statement, DefaultDbSessionFuture toComplete) {
            super(statement,toComplete);
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, Channel channel) throws IOException {
            if (in.read() == RESPONSE_EOF) {
                EofResponse eof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.STATEMENT);
                if (statement.getColumns() == 0) {
                    final StatementPreparedEOF preparedEOF = new StatementPreparedEOF(packetNumber, packetNumber, statement);
                    toComplete.trySetResult(new MySqlPreparedStatement((MySqlConnection) toComplete.getSession(), preparedEOF));
                    return result(new AcceptNextResponse((MySqlConnection) toComplete.getSession()), preparedEOF);
                } else {
                    return result(new ReadColumns(statement.getColumns(), statement, toComplete), statement);
                }
            } else {
                throw new IllegalStateException("Did not expect a EOF from the server");
            }
        }

        @Override
        public String toString() {
            return "PREPARED-STATEMENT-COLUMNS-EOF";
        }
    }

    private static class ReadColumns extends FinishPrepareStatement {
        private final int restOfColumns;

        public ReadColumns(int restOfColumns, PreparedStatementToBuild statement, DefaultDbSessionFuture<MySqlPreparedStatement> toComplete) {
            super(statement,toComplete);
            this.restOfColumns = restOfColumns;
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, Channel channel) throws IOException {
            readAllAndIgnore(in);
            int restOfParams = restOfColumns - 1;
            if (restOfParams > 0) {
                return result(new ReadColumns(restOfParams, statement, toComplete), statement);
            } else {
                return result(new EofStatement(statement,toComplete), statement);
            }
        }

        @Override
        public String toString() {
            return "PREPARED-STATEMENT-READ-COLUMNS";
        }
    }

    private static class EofStatement extends FinishPrepareStatement {

        public EofStatement(PreparedStatementToBuild statement, DefaultDbSessionFuture toComplete) {
            super(statement,toComplete);
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, Channel channel) throws IOException {
            if (in.read() == RESPONSE_EOF) {
                EofResponse eof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.STATEMENT);


                final StatementPreparedEOF preparedEOF = new StatementPreparedEOF(packetNumber, packetNumber, statement);
                toComplete.trySetResult(new MySqlPreparedStatement((MySqlConnection) toComplete.getSession(), preparedEOF));
                return result(new AcceptNextResponse((MySqlConnection) toComplete.getSession()), preparedEOF);
            } else {
                throw new IllegalStateException("Did not expect a EOF from the server");
            }
        }
    }
}
