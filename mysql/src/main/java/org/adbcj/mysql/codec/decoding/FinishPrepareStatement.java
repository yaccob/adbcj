package org.adbcj.mysql.codec.decoding;

import org.adbcj.mysql.codec.AbstractMySqlConnection;
import org.adbcj.mysql.codec.BoundedInputStream;
import org.adbcj.mysql.codec.MysqlType;
import org.adbcj.mysql.codec.decoding.DecoderState;
import org.adbcj.mysql.codec.decoding.FieldDecodingState;
import org.adbcj.mysql.codec.packets.EofResponse;
import org.adbcj.mysql.codec.packets.PreparedStatementToBuild;
import org.adbcj.mysql.codec.packets.StatementPreparedEOF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author roman.stoffel@gamlor.info
 * @since 12.04.12
 */
abstract class FinishPrepareStatement extends DecoderState {

    protected final PreparedStatementToBuild statement;

    FinishPrepareStatement(PreparedStatementToBuild statement) {
        this.statement = statement;
    }

    protected void readAllAndIgnore(BoundedInputStream in) throws IOException {
        in.read(new byte[in.getRemaining()]);
    }

    public static DecoderState create(PreparedStatementToBuild statement) {
        if (statement.getParams() > 0) {
            return new ReadParameters(statement.getParams(), statement);
        } else if (statement.getColumns() > 0) {
            return new ReadColumns(statement.getColumns(), statement);
        } else {
            return RESPONSE;
        }
    }

    private static class ReadParameters extends FinishPrepareStatement {
        private final int parametersToParse;

        public ReadParameters(int parametersToParse, PreparedStatementToBuild statement) {
            super(statement);

            this.parametersToParse = parametersToParse;
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
            int typesCount = statement.getParametersTypes().size();
            MysqlType newType = FieldDecodingState.parseField(in, typesCount).getMysqlType();
            List<MysqlType> types = new ArrayList<MysqlType>(typesCount + 1);
            types.addAll(statement.getParametersTypes());
            types.add(newType);
            PreparedStatementToBuild newStatement = new PreparedStatementToBuild(length, packetNumber,
                    statement.getPreparedStatement(), types);
            int restOfParams = parametersToParse - 1;
            if (restOfParams > 0) {
                return result(new ReadParameters(restOfParams, newStatement), statement);
            } else {
                return result(new EofAndColumns(newStatement), statement);
            }
        }
    }

    private static class EofAndColumns extends FinishPrepareStatement {

        public EofAndColumns(PreparedStatementToBuild statement) {
            super(statement);
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
            if (in.read() == RESPONSE_EOF) {
                EofResponse eof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.STATEMENT);
                if (statement.getColumns() == 0) {
                    return result(RESPONSE, new StatementPreparedEOF(packetNumber, packetNumber, statement));
                } else {
                    return result(new ReadColumns(statement.getColumns(), statement), statement);
                }
            } else {
                throw new IllegalStateException("Did not expect a EOF from the server");
            }
        }
    }

    private static class ReadColumns extends FinishPrepareStatement {

        private final int restOfColumns;

        public ReadColumns(int restOfColumns, PreparedStatementToBuild statement) {
            super(statement);
            this.restOfColumns = restOfColumns;
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
            readAllAndIgnore(in);
            int restOfParams = restOfColumns - 1;
            if (restOfParams > 0) {
                return result(new ReadColumns(restOfParams, statement), statement);
            } else {
                return result(new EofStatement(statement), statement);
            }
        }
    }

    private static class EofStatement extends FinishPrepareStatement {

        public EofStatement(PreparedStatementToBuild statement) {
            super(statement);
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
            if (in.read() == RESPONSE_EOF) {
                EofResponse eof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.STATEMENT);
                return result(RESPONSE, new StatementPreparedEOF(packetNumber, packetNumber, statement));
            } else {
                throw new IllegalStateException("Did not expect a EOF from the server");
            }
        }
    }
}
